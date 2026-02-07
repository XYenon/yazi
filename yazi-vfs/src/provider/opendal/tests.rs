#[cfg(test)]
mod tests {
	use std::io;

	use opendal::{services::Memory, Operator};
	use tokio::io::{AsyncReadExt, AsyncWriteExt};
	use typed_path::UnixPathBuf;
	use yazi_fs::provider::{DirReader, FileHolder, Provider};
	use yazi_shared::{loc::LocBuf, path::PathBufDyn, strand::StrandLike, url::UrlBuf};

	use crate::provider::opendal::OpenDal;

	async fn setup_memory_operator() -> io::Result<Operator> {
		let op = Operator::new(Memory::default())
			.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
			.finish();
		Ok(op)
	}

	fn make_url(path: &str) -> UrlBuf {
		static INIT: std::sync::Once = std::sync::Once::new();
		INIT.call_once(|| {
			yazi_shared::init();
		});

		UrlBuf::OpenDal {
			loc:    LocBuf::from(UnixPathBuf::from(path)),
			domain: yazi_shared::pool::Pool::<str>::intern("test"),
		}
	}

	#[tokio::test]
	async fn test_write_and_read_file() -> io::Result<()> {
		let op = setup_memory_operator().await?;
		let url = make_url("/test_file.txt");
		let provider = OpenDal::new(url.into(), op.clone()).await?;

		let content = b"Hello, OpenDAL!";
		provider.write(content).await?;

		let read_data = op.read("/test_file.txt").await
			.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
		assert_eq!(read_data.to_vec(), content);

		provider.remove_file().await?;
		Ok(())
	}

	#[tokio::test]
	async fn test_copy_file() -> io::Result<()> {
		let op = setup_memory_operator().await?;
		let src_url = make_url("/source.txt");
		let src_provider = OpenDal::new(src_url.into(), op.clone()).await?;

		let content = b"Copy test";
		src_provider.write(content).await?;

		let dst_path = PathBufDyn::Unix("/dest.txt".into());
		let size = src_provider.copy(dst_path, Default::default()).await?;
		assert_eq!(size, content.len() as u64);

		let dst_data = op.read("/dest.txt").await
			.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
		assert_eq!(dst_data.to_vec(), content);

		Ok(())
	}

	#[tokio::test]
	async fn test_rename_file() -> io::Result<()> {
		// Use fs service for rename test since Memory doesn't support it
		let temp_dir = std::env::temp_dir().join(format!("yazi_test_{}", std::process::id()));
		std::fs::create_dir_all(&temp_dir)?;
		
		let op = Operator::via_iter(
			"fs",
			[("root".to_string(), temp_dir.to_str().unwrap().to_string())]
		)
		.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

		let old_url = make_url("/old.txt");
		let provider = OpenDal::new(old_url.into(), op.clone()).await?;

		let content = b"Rename test";
		provider.write(content).await?;

		let new_path = PathBufDyn::Unix("/new.txt".into());
		provider.rename(new_path).await?;

		let new_data = op.read("/new.txt").await
			.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
			.to_vec();
		assert_eq!(new_data, content);

		std::fs::remove_dir_all(&temp_dir).ok();
		Ok(())
	}

	#[tokio::test]
	async fn test_remove_dir_all() -> io::Result<()> {
		let op = setup_memory_operator().await?;

		op.write("/dir/file1.txt", "content1").await
			.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
		op.write("/dir/file2.txt", "content2").await
			.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

		let url = make_url("/dir/");
		let provider = OpenDal::new(url.into(), op.clone()).await?;
		provider.remove_dir_all().await?;

		Ok(())
	}

	#[tokio::test]
	async fn test_read_dir() -> io::Result<()> {
		let op = setup_memory_operator().await?;

		op.write("file1.txt", "c1").await
			.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
		op.write("file2.txt", "c2").await
			.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
		op.write("file3.txt", "c3").await
			.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

		let url = make_url("/");
		let provider = OpenDal::new(url.into(), op.clone()).await?;
		let mut reader = provider.read_dir().await?;

		let mut names = Vec::new();
		while let Some(entry) = reader.next().await? {
			names.push(entry.name().to_string_lossy().into_owned());
		}

		assert!(names.len() >= 3, "Expected at least 3 files, got {}", names.len());

		Ok(())
	}

	#[tokio::test]
	async fn test_open_and_read() -> io::Result<()> {
		let op = setup_memory_operator().await?;
		let url = make_url("/open_test.txt");
		let provider = OpenDal::new(url.into(), op.clone()).await?;

		let content = b"Open test";
		provider.write(content).await?;

		let mut file = provider.open().await?;
		let mut buffer = Vec::new();
		file.read_to_end(&mut buffer).await?;
		assert_eq!(buffer, content);

		Ok(())
	}

	#[tokio::test]
	async fn test_create_and_write() -> io::Result<()> {
		let op = setup_memory_operator().await?;
		let url = make_url("/create_test.txt");
		let provider = OpenDal::new(url.into(), op.clone()).await?;

		let mut file = provider.create().await?;
		let content = b"Create test";
		file.write_all(content).await?;
		file.shutdown().await?;

		let read_data = op.read("/create_test.txt").await
			.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
		assert_eq!(read_data.to_vec(), content);

		Ok(())
	}

	#[tokio::test]
	async fn test_capabilities() {
		let op = Operator::new(Memory::default()).unwrap().finish();
		let url = make_url("/test");
		let provider = OpenDal::new(url.into(), op).await.unwrap();

		let caps = provider.capabilities();
		assert!(!caps.symlink);
	}

	#[tokio::test]
	async fn test_absolute_and_canonicalize() -> io::Result<()> {
		let op = setup_memory_operator().await?;
		let url = make_url("/test_path");
		let provider = OpenDal::new(url.clone().into(), op).await?;

		let abs = provider.absolute().await?;
		assert_eq!(abs.to_owned(), url);

		let canon = provider.canonicalize().await?;
		assert_eq!(canon, url);

		Ok(())
	}

	#[tokio::test]
	async fn test_unsupported_operations() -> io::Result<()> {
		let op = setup_memory_operator().await?;
		let url = make_url("/test");
		let provider = OpenDal::new(url.into(), op).await?;

		assert!(provider.read_link().await.is_err());
		assert!(provider.trash().await.is_err());
		assert!(provider.create_new().await.is_err());
		assert!(provider.symlink("target", || async { Ok(false) }).await.is_err());
		assert!(provider.symlink_dir("target").await.is_err());
		assert!(provider.symlink_file("target").await.is_err());

		Ok(())
	}

	#[tokio::test]
	async fn test_file_metadata() -> io::Result<()> {
		let op = setup_memory_operator().await?;
		let url = make_url("/metadata_test.txt");
		let provider = OpenDal::new(url.into(), op.clone()).await?;

		let content = b"Test metadata";
		let mut file = provider.create().await?;
		file.write_all(content).await?;
		file.flush().await?;

		let cha = file.metadata().await?;
		assert_eq!(cha.len, content.len() as u64);
		assert!(cha.mode.contains(yazi_fs::cha::ChaMode::T_FILE));

		Ok(())
	}

	#[tokio::test]
	async fn test_copy_with_progress() -> io::Result<()> {
		let op = setup_memory_operator().await?;
		let src_url = make_url("/progress_src.txt");
		let src_provider = OpenDal::new(src_url.into(), op.clone()).await?;

		let content = vec![0u8; 20 * 1024 * 1024]; // 20MB to trigger progress updates
		src_provider.write(&content).await?;

		let dst_path = PathBufDyn::Unix("/progress_dst.txt".into());
		let mut rx = src_provider.copy_with_progress(dst_path, yazi_fs::provider::Attrs::default())?;

		let mut last_progress = 0u64;
		while let Some(result) = rx.recv().await {
			let progress = result?;
			assert!(progress >= last_progress);
			last_progress = progress;
		}

		assert_eq!(last_progress, content.len() as u64);

		let dst_data = op.read("/progress_dst.txt").await
			.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
		assert_eq!(dst_data.len(), content.len());

		Ok(())
	}
}
