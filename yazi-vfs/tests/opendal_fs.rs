use std::{
	io,
	path::{Path, PathBuf},
	str::FromStr,
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use yazi_fs::{
	FsUrl,
	cha::ChaType,
	provider::{DirReader, FileBuilder, FileHolder},
};
use yazi_shared::{
	loc::LocBuf,
	pool::InternStr,
	scheme::SchemeKind,
	url::{AsUrl, UrlBuf},
};

fn init_ctx() -> (&'static PathBuf, &'static PathBuf) {
	static INIT: std::sync::OnceLock<(PathBuf, PathBuf)> = std::sync::OnceLock::new();

	let (config_dir, remote_root) = INIT.get_or_init(|| {
		yazi_shared::init_tests();
		yazi_fs::init();
		yazi_vfs::init();

		let uniq = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
		let base = std::env::temp_dir().join(format!("yazi-opendal-fs-{uniq}-{}", std::process::id()));

		let config_dir = base.join("config");
		let remote_root = base.join("remote");
		std::fs::create_dir_all(&config_dir).unwrap();
		std::fs::create_dir_all(&remote_root).unwrap();

		unsafe {
			std::env::set_var("YAZI_CONFIG_HOME", &config_dir);
		}

		let uri = format!("fs://{}", remote_root.display());
		std::fs::write(
			config_dir.join("vfs.toml"),
			format!(
				r#"[services]

[services.testfs]
type   = "opendal"
scheme = "fs"
root   = "{uri}"

[services.memfs]
type   = "opendal"
scheme = "memory"
"#
			),
		)
		.unwrap();

		(config_dir, remote_root)
	});

	(config_dir, remote_root)
}

fn uniq(prefix: &str) -> String {
	let uniq = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
	format!("{prefix}{uniq}-{}", std::process::id())
}

async fn wait_until(timeout: Duration, mut f: impl FnMut() -> bool) {
	let deadline = tokio::time::Instant::now() + timeout;
	while tokio::time::Instant::now() < deadline {
		if f() {
			return;
		}
		tokio::time::sleep(Duration::from_millis(25)).await;
	}
	assert!(f(), "condition not met within {timeout:?}");
}

async fn wait_remote_bytes(remote_root: &Path, rel: &str, expected: &[u8]) {
	let p = remote_root.join(rel);
	wait_until(Duration::from_secs(2), || p.exists()).await;
	wait_until(Duration::from_secs(2), || std::fs::read(&p).ok().as_deref() == Some(expected)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn opendal_fs_full() {
	let (_config_dir, remote_root) = init_ctx();

	let base = uniq("opendal-full-");

	let file_url = UrlBuf::from_str(&format!("opendal://testfs//{base}/hello.txt")).unwrap();
	let file_url2 = UrlBuf::from_str(&format!("opendal://testfs//{base}/drop.txt")).unwrap();
	let file_url3 = UrlBuf::from_str(&format!("opendal://testfs//{base}/new.txt")).unwrap();
	let file_url3_renamed =
		UrlBuf::from_str(&format!("opendal://testfs//{base}/renamed.txt")).unwrap();
	let file_url3_copied = UrlBuf::from_str(&format!("opendal://testfs//{base}/copied.txt")).unwrap();

	let empty_dir_url = UrlBuf::from_str(&format!("opendal://testfs//{base}/empty/")).unwrap();
	let tree_dir_url = UrlBuf::from_str(&format!("opendal://testfs//{base}/a/b/")).unwrap();
	let tree_file_url = UrlBuf::from_str(&format!("opendal://testfs//{base}/a/b/file.txt")).unwrap();

	// Write file via VFS and wait for upload to finish by calling shutdown().
	let mut f = yazi_vfs::provider::create(&file_url).await.unwrap();
	f.write_all(b"hello").await.unwrap();
	f.shutdown().await.unwrap();

	wait_remote_bytes(remote_root, &format!("{base}/hello.txt"), b"hello").await;

	// Drop local cache to force download-on-open.
	let cache_path = file_url.as_url().cache().unwrap();
	let _ = tokio::fs::remove_file(&cache_path).await;

	let mut f = yazi_vfs::provider::open(&file_url).await.unwrap();
	let mut buf = Vec::new();
	f.read_to_end(&mut buf).await.unwrap();
	assert_eq!(buf, b"hello");

	// Upload on Drop (best-effort spawn): no explicit shutdown.
	{
		let mut f = yazi_vfs::provider::create(&file_url2).await.unwrap();
		f.write_all(b"drop").await.unwrap();
	}
	wait_remote_bytes(remote_root, &format!("{base}/drop.txt"), b"drop").await;

	// create_new: ok for new file, AlreadyExists for existing.
	{
		let mut f = yazi_vfs::provider::create_new(&file_url3).await.unwrap();
		f.write_all(b"new").await.unwrap();
		f.shutdown().await.unwrap();
	}
	wait_remote_bytes(remote_root, &format!("{base}/new.txt"), b"new").await;
	let err = match yazi_vfs::provider::create_new(&file_url3).await {
		Ok(_) => panic!("expected AlreadyExists for create_new"),
		Err(e) => e,
	};
	assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

	// create_new shouldn't be blocked by a stale local cache file when the remote is missing.
	let stale_url = UrlBuf::from_str(&format!("opendal://testfs//{base}/stale.txt")).unwrap();
	{
		let mut f = yazi_vfs::provider::create(&stale_url).await.unwrap();
		f.write_all(b"stale").await.unwrap();
		f.shutdown().await.unwrap();
	}
	wait_remote_bytes(remote_root, &format!("{base}/stale.txt"), b"stale").await;
	yazi_vfs::provider::remove_file(&stale_url).await.unwrap();
	wait_until(Duration::from_secs(2), || !remote_root.join(format!("{base}/stale.txt")).exists())
		.await;

	let cache_path = stale_url.as_url().cache().unwrap();
	if !tokio::fs::try_exists(&cache_path).await.unwrap_or(false) {
		tokio::fs::File::create(&cache_path).await.unwrap();
	}

	{
		let mut f = yazi_vfs::provider::create_new(&stale_url).await.unwrap();
		f.write_all(b"fresh").await.unwrap();
		f.shutdown().await.unwrap();
	}
	wait_remote_bytes(remote_root, &format!("{base}/stale.txt"), b"fresh").await;

	// open: NotFound when remote missing and cache missing.
	let missing_url = UrlBuf::from_str(&format!("opendal://testfs//{base}/missing.txt")).unwrap();
	let err = match yazi_vfs::provider::open(&missing_url).await {
		Ok(_) => panic!("expected NotFound for open"),
		Err(e) => e,
	};
	assert_eq!(err.kind(), io::ErrorKind::NotFound);

	// Append forces download (when cache missing) and then uploads full file.
	let _ = tokio::fs::remove_file(file_url.as_url().cache().unwrap()).await;
	let mut f = yazi_vfs::provider::Gate::default()
		.read(true)
		.write(true)
		.append(true)
		.open(&file_url)
		.await
		.unwrap();
	f.write_all(b" world").await.unwrap();
	f.shutdown().await.unwrap();
	wait_remote_bytes(remote_root, &format!("{base}/hello.txt"), b"hello world").await;

	// Truncate skips download and uploads truncated contents.
	let _ = tokio::fs::remove_file(file_url.as_url().cache().unwrap()).await;
	let mut f =
		yazi_vfs::provider::Gate::default().write(true).truncate(true).open(&file_url).await.unwrap();
	f.write_all(b"x").await.unwrap();
	f.shutdown().await.unwrap();
	wait_remote_bytes(remote_root, &format!("{base}/hello.txt"), b"x").await;

	// rename and copy (file).
	yazi_vfs::provider::rename(&file_url3, &file_url3_renamed).await.unwrap();
	assert!(
		!tokio::fs::try_exists(remote_root.join(format!("{base}/new.txt"))).await.unwrap_or(false)
	);
	wait_remote_bytes(remote_root, &format!("{base}/renamed.txt"), b"new").await;

	let copied = yazi_vfs::provider::copy(
		&file_url3_renamed,
		&file_url3_copied,
		yazi_fs::provider::Attrs::default(),
	)
	.await
	.unwrap();
	assert_eq!(copied, 3);
	wait_remote_bytes(remote_root, &format!("{base}/copied.txt"), b"new").await;

	// remove_file
	yazi_vfs::provider::remove_file(&file_url3_copied).await.unwrap();
	assert!(
		!tokio::fs::try_exists(remote_root.join(format!("{base}/copied.txt"))).await.unwrap_or(false)
	);

	// create_dir / remove_dir (empty dir).
	yazi_vfs::provider::create_dir(&empty_dir_url).await.unwrap();
	assert!(tokio::fs::try_exists(remote_root.join(format!("{base}/empty"))).await.unwrap());
	yazi_vfs::provider::remove_dir(&empty_dir_url).await.unwrap();
	assert!(!tokio::fs::try_exists(remote_root.join(format!("{base}/empty"))).await.unwrap_or(false));

	// Create directory tree and check its metadata.
	yazi_vfs::provider::create_dir_all(&tree_dir_url).await.unwrap();
	assert!(tokio::fs::try_exists(remote_root.join(format!("{base}/a/b"))).await.unwrap());
	let cha = yazi_vfs::provider::metadata(&tree_dir_url).await.unwrap();
	assert_eq!(**cha, ChaType::Dir);

	// remove_dir_all (via default recursion).
	{
		let mut f = yazi_vfs::provider::create(&tree_file_url).await.unwrap();
		f.write_all(b"deep").await.unwrap();
		f.shutdown().await.unwrap();
	}
	wait_remote_bytes(remote_root, &format!("{base}/a/b/file.txt"), b"deep").await;
	yazi_vfs::provider::remove_dir_all(
		&UrlBuf::from_str(&format!("opendal://testfs//{base}/a/")).unwrap(),
	)
	.await
	.unwrap();
	assert!(!tokio::fs::try_exists(remote_root.join(format!("{base}/a"))).await.unwrap_or(false));

	// casefold: resolve path by scanning parent directory.
	let mixed = UrlBuf::from_str(&format!("opendal://testfs//{base}/MiXeD.TXT")).unwrap();
	{
		let mut f = yazi_vfs::provider::create(&mixed).await.unwrap();
		f.write_all(b"casefold").await.unwrap();
		f.shutdown().await.unwrap();
	}
	let lower = UrlBuf::from_str(&format!("opendal://testfs//{base}/mixed.txt")).unwrap();
	let resolved = yazi_vfs::provider::casefold(&lower).await.unwrap();
	assert_eq!(resolved, mixed);

	let mut f = yazi_vfs::provider::open(&resolved).await.unwrap();
	let mut buf = Vec::new();
	f.read_to_end(&mut buf).await.unwrap();
	assert_eq!(buf, b"casefold");

	// Non-UTF8 names should fail fast instead of lossy remote key conversion.
	let bad_loc = LocBuf::<typed_path::UnixPathBuf>::saturated(
		typed_path::UnixPathBuf::from(vec![b'/', 0xff, b'a']),
		SchemeKind::Opendal,
	);
	let bad_url = UrlBuf::Opendal { loc: bad_loc, domain: "testfs".intern() };
	let err = match yazi_vfs::provider::metadata(&bad_url).await {
		Ok(_) => panic!("expected InvalidInput for non-UTF8 OpenDAL path"),
		Err(e) => e,
	};
	assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

	// Synthesized directory entries shouldn't require a backing "directory marker" object.
	let mem_base = uniq("opendal-mem-");
	let mem_deep =
		UrlBuf::from_str(&format!("opendal://memfs//{mem_base}/dir1/dir2/file.txt")).unwrap();
	{
		let mut f = yazi_vfs::provider::create(&mem_deep).await.unwrap();
		f.write_all(b"x").await.unwrap();
		f.shutdown().await.unwrap();
	}

	let mem_root = UrlBuf::from_str(&format!("opendal://memfs//{mem_base}/")).unwrap();
	let mut rd = yazi_vfs::provider::read_dir(&mem_root).await.unwrap();
	let mut dir1 = None;
	while let Some(ent) = rd.next().await.unwrap() {
		if ent.name().into_string_lossy() == "dir1" {
			dir1 = Some(ent);
			break;
		}
	}
	let dir1 = dir1.expect("expected synthesized `dir1/` entry to be listed");
	let cha = dir1.metadata().await.unwrap();
	assert_eq!(**cha, ChaType::Dir);

	// List root directory.
	let root_url = UrlBuf::from_str(&format!("opendal://testfs//{base}/")).unwrap();
	let mut rd = yazi_vfs::provider::read_dir(&root_url).await.unwrap();
	let mut names = Vec::new();
	while let Some(ent) = rd.next().await.unwrap() {
		names.push(ent.name().into_string_lossy());
	}
	assert!(names.iter().any(|n| n == "hello.txt"), "entries: {names:?}");
	assert!(names.iter().any(|n| n == "drop.txt"), "entries: {names:?}");
	assert!(names.iter().any(|n| n == "renamed.txt"), "entries: {names:?}");
	assert!(names.iter().any(|n| n == "MiXeD.TXT"), "entries: {names:?}");
}
