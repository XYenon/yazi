use std::{io, sync::LazyLock};

use hashbrown::HashMap;
use opendal::Operator;
use parking_lot::Mutex;
use yazi_config::vfs::{ServiceOpenDal, Vfs};
use yazi_fs::{cha::{Cha, ChaKind, ChaMode}, provider::{Attrs, Capabilities, Provider}};
use yazi_shared::{path::{AsPath, PathBufDyn}, strand::AsStrand, url::{AsUrl, Url, UrlBuf, UrlCow, UrlLike}};

const MAX_CACHED_OPERATORS: usize = 64;

static OPERATOR_CACHE: LazyLock<Mutex<(HashMap<String, (Operator, CacheKey)>, std::collections::VecDeque<String>)>> =
	LazyLock::new(|| Mutex::new((HashMap::new(), std::collections::VecDeque::new())));

#[derive(Clone, PartialEq, Eq)]
struct CacheKey {
	backend: String,
	options: std::collections::BTreeMap<String, String>,
}

impl CacheKey {
	fn from_config(config: &ServiceOpenDal) -> Self {
		Self { backend: config.backend.clone(), options: config.options.iter().map(|(k, v)| (k.clone(), v.clone())).collect() }
	}
}

mod dir_entry;
mod file;
mod read_dir;
mod tests;

pub use dir_entry::DirEntry;
pub use file::File;
pub use read_dir::ReadDir;

pub(crate) fn meta_to_cha(meta: &opendal::Metadata) -> Cha {
	let kind = ChaKind::empty();
	let mode = if meta.is_dir() {
		ChaMode::T_DIR
	} else if meta.is_file() {
		ChaMode::T_FILE
	} else {
		ChaMode::empty()
	};

	let mtime = meta.last_modified()
		.and_then(|t| {
			use std::time::SystemTime;
			SystemTime::try_from(t).ok()
		});

	Cha {
		kind,
		mode,
		len: meta.content_length(),
		atime: None,
		btime: None,
		ctime: None,
		mtime,
		dev: 0,
		uid: 0,
		gid: 0,
		nlink: 0,
	}
}

#[derive(Clone)]
pub struct OpenDal<'a> {
	url: UrlCow<'a>,
	op: Operator,
}

impl<'a> OpenDal<'a> {
	pub async fn new(url: UrlCow<'a>, op: Operator) -> io::Result<Self> {
		Ok(Self { url, op })
	}

	pub async fn from_url(url: Url<'a>) -> io::Result<Self> {
		let domain = url.scheme().domain().ok_or_else(|| {
			io::Error::new(io::ErrorKind::InvalidInput, "OpenDal URL must have a domain")
		})?;

		let (_name, config) = Vfs::service::<&ServiceOpenDal>(domain).await?;
		let key = CacheKey::from_config(config);

		{
			let mut cache = OPERATOR_CACHE.lock();
			if let Some((op, cached_key)) = cache.0.get(domain) {
				if *cached_key == key {
					return Ok(Self { url: url.into(), op: op.clone() });
				}
			}
		}

		let op = Self::build_operator(config)?;
		let mut cache = OPERATOR_CACHE.lock();
		if !cache.0.contains_key(domain) {
			if cache.0.len() >= MAX_CACHED_OPERATORS {
				if let Some(evict) = cache.1.pop_front() {
					cache.0.remove(&evict);
				}
			}
			cache.1.push_back(domain.to_owned());
		}
		cache.0.insert(domain.to_owned(), (op.clone(), key));

		Ok(Self { url: url.into(), op })
	}

	fn build_operator(config: &ServiceOpenDal) -> io::Result<Operator> {
		let mut map = std::collections::HashMap::new();
		for (k, v) in &config.options {
			map.insert(k.clone(), v.clone());
		}

		Operator::via_iter(&config.backend, map)
			.map_err(|e| io::Error::other(format!("failed to build opendal operator: {e}")))
	}

	fn path_str(&self) -> io::Result<String> {
		self.url
			.as_url()
			.loc()
			.as_strand()
			.as_utf8()
			.map(|s| s.to_owned())
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path is not valid UTF-8"))
	}
}

/// Internal helper for copying with optional progress tracking.
/// Returns the total bytes copied on success.
async fn do_copy(
	op: Operator,
	from_path: String,
	to_path: String,
	progress_tx: Option<tokio::sync::mpsc::Sender<Result<u64, io::Error>>>,
) -> io::Result<u64> {

	// Try server-side copy first
	match op.copy(&from_path, &to_path).await {
		Ok(_) => {
			let meta = op.stat(&to_path).await.map_err(io::Error::other)?;
			let size = meta.content_length();
			if let Some(tx) = progress_tx {
				let _ = tx.send(Ok(size)).await;
			}
			return Ok(size);
		}
		Err(e) if e.kind() == opendal::ErrorKind::Unsupported => {}
		Err(e) => {
			let err = io::Error::other(e);
			if let Some(tx) = progress_tx {
				let _ = tx.send(Err(io::Error::other(format!("{err}")))).await;
			}
			return Err(err);
		}
	}

	// Fallback: streaming copy via chunked reads
	let meta = op.stat(&from_path).await.map_err(io::Error::other)?;
	let total_size = meta.content_length();
	let reader = op.reader(&from_path).await.map_err(io::Error::other)?;
	let mut writer = op.writer(&to_path).await.map_err(io::Error::other)?;

	const CHUNK: u64 = 8 * 1024 * 1024;
	let mut offset = 0u64;
	let mut failed = false;

	while offset < total_size {
		let end = (offset + CHUNK).min(total_size);
		let buf = match reader.read(offset..end).await {
			Ok(b) => b,
			Err(e) => {
				let err = io::Error::other(e);
				if let Some(tx) = &progress_tx {
					let _ = tx.send(Err(io::Error::other(format!("{err}")))).await;
				}
				failed = Some(err);
				break;
			}
		};

		let len = buf.len() as u64;
		if let Err(e) = writer.write(buf).await {
			let err = io::Error::other(e);
			if let Some(tx) = &progress_tx {
				let _ = tx.send(Err(io::Error::other(format!("{err}")))).await;
			}
			failed = Some(err);
			break;
		}
		offset = end;
		if let Some(tx) = &progress_tx {
			if tx.send(Ok(len)).await.is_err() {
				failed = Some(io::Error::new(io::ErrorKind::BrokenPipe, "progress pipe closed"));
				break;
			}
		}
	}

	if let Some(e) = failed {
		drop(writer);
		let _ = op.delete(&to_path).await;
		return Err(e);
	}

	if let Err(e) = writer.close().await {
		let err = io::Error::other(e);
		if let Some(tx) = &progress_tx {
			let _ = tx.send(Err(io::Error::other(format!("{err}")))).await;
		}
		let _ = op.delete(&to_path).await;
		return Err(err);
	}

	Ok(total_size)
}

impl<'a> Provider for OpenDal<'a> {
	type File = super::RwFile;
	type Gate = super::Gate;
	type Me<'b> = OpenDal<'b>;
	type ReadDir = ReadDir;
	type UrlCow = UrlCow<'a>;

	async fn absolute(&self) -> io::Result<Self::UrlCow> {
		Ok(self.url.clone())
	}

	async fn canonicalize(&self) -> io::Result<UrlBuf> {
		Ok(self.url.to_owned())
	}

	fn capabilities(&self) -> Capabilities {
		Capabilities { symlink: false }
	}

	async fn casefold(&self) -> io::Result<UrlBuf> {
		Ok(self.url.to_owned())
	}

	async fn copy<P>(&self, to: P, _attrs: Attrs) -> io::Result<u64>
	where
		P: AsPath,
	{
		let from_path = self.path_str()?;
		let to_path = to
			.as_path()
			.as_strand()
			.as_utf8()
			.map(|s| s.to_owned())
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "destination path is not valid UTF-8"))?;

		do_copy(self.op.clone(), from_path, to_path, None).await
	}

	fn copy_with_progress<P, A>(
		&self,
		to: P,
		_attrs: A,
	) -> io::Result<tokio::sync::mpsc::Receiver<Result<u64, io::Error>>>
	where
		P: AsPath,
		A: Into<Attrs>,
	{
		let from_path = self.path_str()?;
		let to_path = to
			.as_path()
			.as_strand()
			.as_utf8()
			.map(|s| s.to_owned())
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "destination path is not valid UTF-8"))?;
		let op = self.op.clone();

		let (tx, rx) = tokio::sync::mpsc::channel(1);

		tokio::spawn(async move {
			let _ = do_copy(op, from_path, to_path, Some(tx)).await;
		});

		Ok(rx)
	}

	async fn create(&self) -> io::Result<Self::File> {
		let path = self.path_str()?;
		Ok(File::create(self.op.clone(), path).await?.into())
	}

	async fn create_dir(&self) -> io::Result<()> {
		let path = self.path_str()?;
		self.op.create_dir(&path).await.map_err(io::Error::other)
	}

	async fn create_dir_all(&self) -> io::Result<()> {
		let path = self.path_str()?;
		let mut current = if path.starts_with('/') { "/" } else { "" }.to_string();
		for component in path.split('/').filter(|c| !c.is_empty() && *c != ".") {
			if component == ".." {
				return Err(io::Error::new(io::ErrorKind::InvalidInput, "path contains '..' component"));
			}
			if !current.is_empty() && !current.ends_with('/') {
				current.push('/');
			}
			current.push_str(component);
			current.push('/');

			match self.op.stat(&current).await {
				Ok(m) if m.is_dir() => continue,
				Ok(_) => {
					return Err(io::Error::new(
						io::ErrorKind::AlreadyExists,
						format!("path already exists and is not a directory: {current}"),
					))
				}
				Err(e) if e.kind() == opendal::ErrorKind::NotFound => {}
				Err(e) => return Err(io::Error::other(e)),
			}

			match self.op.create_dir(&current).await {
				Ok(_) => {}
				Err(e) if e.kind() == opendal::ErrorKind::AlreadyExists => {}
				Err(e) => return Err(io::Error::other(e)),
			}
		}
		Ok(())
	}

	async fn create_new(&self) -> io::Result<Self::File> {
		Err(io::Error::new(io::ErrorKind::Unsupported, "atomic create_new not supported"))
	}

	async fn hard_link<P>(&self, _link: P) -> io::Result<()>
	where
		P: AsPath,
	{
		Err(io::Error::new(io::ErrorKind::Unsupported, "hard_link not supported"))
	}

	async fn metadata(&self) -> io::Result<Cha> {
		let path = self.path_str()?;
		let meta = self.op.stat(&path).await.map_err(io::Error::other)?;

		Ok(meta_to_cha(&meta))
	}

	async fn new<'b>(url: Url<'b>) -> io::Result<Self::Me<'b>> {
		Self::Me::from_url(url).await
	}

	async fn open(&self) -> io::Result<Self::File> {
		let path = self.path_str()?;
		Ok(File::open(self.op.clone(), path).await?.into())
	}

	async fn read_dir(self) -> io::Result<Self::ReadDir> {
		let path = self.path_str()?;
		let domain = self.url.scheme().domain().ok_or_else(|| {
			io::Error::new(io::ErrorKind::InvalidInput, "OpenDal URL must have a domain")
		})?;

		ReadDir::new(self.op, path, domain.to_owned()).await
	}

	async fn read_link(&self) -> io::Result<PathBufDyn> {
		Err(io::Error::new(io::ErrorKind::Unsupported, "read_link not supported"))
	}

	async fn remove_dir(&self) -> io::Result<()> {
		let path = self.path_str()?;
		self.op.delete(&path).await.map_err(io::Error::other)
	}

	async fn remove_dir_all(&self) -> io::Result<()> {
		let path = self.path_str()?;
		self.op.remove_all(&path).await.map_err(io::Error::other)
	}

	async fn remove_dir_clean(&self) -> io::Result<()> {
		self.remove_dir().await
	}

	async fn remove_file(&self) -> io::Result<()> {
		let path = self.path_str()?;
		self.op.delete(&path).await.map_err(io::Error::other)
	}

	async fn rename<P>(&self, to: P) -> io::Result<()>
	where
		P: AsPath,
	{
		let from_path = self.path_str()?;
		let to_path = to
			.as_path()
			.as_strand()
			.as_utf8()
			.map(|s| s.to_owned())
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "destination path is not valid UTF-8"))?;

		match self.op.rename(&from_path, &to_path).await {
			Ok(_) => Ok(()),
			Err(e) if e.kind() == opendal::ErrorKind::Unsupported => {
				do_copy(self.op.clone(), from_path.clone(), to_path, None).await?;
				self.op.delete(&from_path).await.map_err(io::Error::other)
			}
			Err(e) => Err(io::Error::other(e)),
		}
	}

	async fn symlink<S, F>(&self, _original: S, _is_dir: F) -> io::Result<()>
	where
		S: AsStrand,
		F: AsyncFnOnce() -> io::Result<bool>,
	{
		Err(io::Error::new(io::ErrorKind::Unsupported, "symlink not supported"))
	}

	async fn symlink_dir<S>(&self, _original: S) -> io::Result<()>
	where
		S: AsStrand,
	{
		Err(io::Error::new(io::ErrorKind::Unsupported, "symlink_dir not supported"))
	}

	async fn symlink_file<S>(&self, _original: S) -> io::Result<()>
	where
		S: AsStrand,
	{
		Err(io::Error::new(io::ErrorKind::Unsupported, "symlink_file not supported"))
	}

	async fn symlink_metadata(&self) -> io::Result<Cha> {
		self.metadata().await
	}

	async fn trash(&self) -> io::Result<()> {
		Err(io::Error::new(io::ErrorKind::Unsupported, "trash not supported"))
	}

	fn url(&self) -> Url<'_> {
		self.url.as_url()
	}

	async fn write<C>(&self, contents: C) -> io::Result<()>
	where
		C: AsRef<[u8]>,
	{
		let path = self.path_str()?;
		let data = contents.as_ref().to_vec();
		self.op.write(&path, data).await.map_err(io::Error::other)?;
		Ok(())
	}
}

