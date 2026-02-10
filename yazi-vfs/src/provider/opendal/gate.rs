use std::{io, sync::Arc};

use futures::TryStreamExt;
use yazi_config::vfs::{ServiceOpendal, Vfs};
use yazi_fs::provider::{Attrs, FileBuilder};
use yazi_fs::FsUrl;
use yazi_shared::url::{AsUrl, Url};

use super::file::{File, WriteSession};

#[derive(Clone, Copy, Default)]
pub struct Gate(crate::provider::Gate);

impl FileBuilder for Gate {
	type File = File;

	fn append(&mut self, append: bool) -> &mut Self {
		self.0.append(append);
		self
	}

	fn attrs(&mut self, attrs: Attrs) -> &mut Self {
		self.0.attrs(attrs);
		self
	}

	fn create(&mut self, create: bool) -> &mut Self {
		self.0.create(create);
		self
	}

	fn create_new(&mut self, create_new: bool) -> &mut Self {
		self.0.create_new(create_new);
		self
	}

	async fn open<U>(&self, url: U) -> io::Result<Self::File>
	where
		U: AsUrl,
	{
		use tokio::io::AsyncWriteExt;

		let url = url.as_url();
		let (path, (name, config)) = match url {
			Url::Opendal { loc, domain } => (*loc, Vfs::service::<&ServiceOpendal>(domain).await?),
			_ => Err(io::Error::new(io::ErrorKind::InvalidInput, format!("Not an OpenDAL URL: {url:?}")))?,
		};

		let op = {
			let mut ops = super::OPS.lock();
			if let Some(op) = ops.get(name).cloned() {
				op
			} else {
				let extra = config.options.iter().map(|(k, v)| (k.as_str(), v.as_str()));
				let op = ::opendal::Operator::from_uri((config.uri.as_str(), extra)).map_err(io::Error::from)?;
				ops.insert(name, op.clone());
				op
			}
		};

		let key = super::key_from_unix(path)?;

		let cache = url.cache().ok_or_else(|| io::Error::other("OpenDAL URL has no cache path"))?;
		if let Some(parent) = cache.parent() {
			tokio::fs::create_dir_all(parent).await?;
		}

		let remote_exists = if key.is_empty() {
			true
		} else {
			match op.stat(&key).await {
				Ok(_) => true,
				Err(e) if e.kind() == ::opendal::ErrorKind::NotFound => false,
				Err(e) => return Err(io::Error::from(e)),
			}
		};

		if self.0.create_new && remote_exists {
			return Err(io::Error::from(io::ErrorKind::AlreadyExists));
		}

		let cache_exists = tokio::fs::try_exists(&cache).await.unwrap_or(false);
		let want_local_source = self.0.read || self.0.append || (self.0.write && !self.0.truncate);

		if want_local_source && !cache_exists && remote_exists {
			let mut f = tokio::fs::File::create(&cache).await?;
			let mut stream = op.reader(&key).await.map_err(io::Error::from)?.into_stream(..).await.map_err(io::Error::from)?;
			while let Some(buf) = stream.try_next().await.map_err(io::Error::from)? {
				for bs in buf {
					f.write_all(bs.as_ref()).await?;
				}
			}
			f.flush().await.ok();
		} else if self.0.truncate && !cache_exists && remote_exists {
			// Truncating an existing remote file shouldn't depend on having a local cache.
			// Create an empty cache file so `OpenOptions::truncate(true)` can succeed.
			tokio::fs::File::create(&cache).await?;
		} else if self.0.read && !cache_exists {
			return Err(io::Error::from(io::ErrorKind::NotFound));
		}

		#[cfg(unix)]
		if tokio::fs::try_exists(&cache).await.unwrap_or(false) {
			use std::os::unix::fs::PermissionsExt;
			if let Ok(meta) = tokio::fs::metadata(&cache).await {
				let perm = meta.permissions();
				if perm.mode() & 0o600 != 0o600 {
					tokio::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o644)).await.ok();
				}
			}
		}

		let mut opts = tokio::fs::OpenOptions::new();
		opts.read(self.0.read);
		opts.write(self.0.write);
		opts.append(self.0.append);
		opts.truncate(self.0.truncate);
		opts.create(self.0.create);
		opts.create_new(self.0.create_new);

		let file = opts.open(&cache).await?;

		let session = if self.0.write || self.0.append || self.0.create || self.0.truncate || self.0.create_new {
			let mut sessions = super::SESSIONS.lock();
			let s = sessions
				.entry(cache.clone())
				.or_insert_with(|| Arc::new(WriteSession::new(op.clone(), key.clone(), cache.clone())))
				.clone();
			s.writers.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
			Some(s)
		} else {
			None
		};

		Ok(File::new(file, session))
	}

	fn read(&mut self, read: bool) -> &mut Self {
		self.0.read(read);
		self
	}

	fn truncate(&mut self, truncate: bool) -> &mut Self {
		self.0.truncate(truncate);
		self
	}

	fn write(&mut self, write: bool) -> &mut Self {
		self.0.write(write);
		self
	}
}
