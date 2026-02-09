use std::{io, sync::Arc};

use tokio::sync::mpsc::Receiver;
use yazi_config::vfs::{ServiceOpendal, Vfs};
use yazi_fs::{cha::{Cha, ChaKind, ChaMode}, provider::{Capabilities, DirReader, FileHolder, Provider}};
use yazi_shared::{loc::LocBuf, path::{AsPath, PathBufDyn}, pool::InternStr, scheme::SchemeKind, strand::AsStrand, url::{Url, UrlBuf, UrlCow, UrlLike}};

use crate::provider::opendal::absolute;

pub fn cha_from_meta(url: &UrlBuf, meta: &::opendal::Metadata) -> Cha {
	let mut kind = ChaKind::empty();
	if url.urn().is_hidden() {
		kind |= ChaKind::HIDDEN;
	}

	let mode = match meta.mode() {
		::opendal::EntryMode::FILE => ChaMode::T_FILE,
		::opendal::EntryMode::DIR => ChaMode::T_DIR,
		::opendal::EntryMode::Unknown => ChaMode::empty(),
	};

	Cha {
		kind,
		mode,
		len: meta.content_length(),
		mtime: meta.last_modified().map(Into::into),
		nlink: 1,
		..Default::default()
	}
}

#[derive(Clone)]
pub struct Opendal<'a> {
	url:  Url<'a>,
	path: &'a typed_path::UnixPath,

	name:   &'static str,
	config: &'static ServiceOpendal,
}

impl<'a> Opendal<'a> {
	fn key(&self) -> String {
		let mut key = String::from_utf8_lossy(self.path.as_bytes()).into_owned();
		key = key.trim_start_matches('/').to_owned();
		key
	}

	async fn op(&self) -> io::Result<::opendal::Operator> {
		let mut ops = super::OPS.lock();
		if let Some(op) = ops.get(self.name).cloned() {
			return Ok(op);
		}

		let extra = self.config.options.iter().map(|(k, v)| (k.as_str(), v.as_str()));
		let op = ::opendal::Operator::from_uri((self.config.uri.as_str(), extra)).map_err(io::Error::from)?;
		ops.insert(self.name, op.clone());
		Ok(op)
	}
}

impl<'a> Provider for Opendal<'a> {
	type File = super::file::File;
	type Gate = super::gate::Gate;
	type Me<'b> = Opendal<'b>;
	type ReadDir = super::read_dir::ReadDir;
	type UrlCow = UrlCow<'a>;

	async fn absolute(&self) -> io::Result<Self::UrlCow> {
		Ok(if let Some(u) = absolute::try_absolute(self.url) {
			u
		} else {
			self.canonicalize().await?.into()
		})
	}

	async fn canonicalize(&self) -> io::Result<UrlBuf> { Ok(self.url.to_owned()) }

	fn capabilities(&self) -> Capabilities { Capabilities { symlink: false } }

	async fn casefold(&self) -> io::Result<UrlBuf> {
		let Some((parent, name)) = self.url.parent().zip(self.url.name()) else {
			return Ok(self.url.to_owned());
		};

		let mut it = Self::new(parent).await?.read_dir().await?;
		let mut similar = None;
		while let Some(entry) = it.next().await? {
			let s = entry.name();
			if !name.eq_ignore_ascii_case(&s) {
				continue;
			} else if s == name {
				return Ok(entry.url());
			} else if similar.is_none() {
				similar = Some(s.into_owned());
			} else {
				return Err(io::Error::from(io::ErrorKind::NotFound));
			}
		}

		similar
			.map(|n| parent.try_join(n))
			.transpose()?
			.ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
	}

	async fn copy<P>(&self, to: P, _attrs: yazi_fs::provider::Attrs) -> io::Result<u64>
	where
		P: AsPath,
	{
		let to = to.as_path().as_unix()?;
		let to = String::from_utf8_lossy(to.as_bytes()).trim_start_matches('/').to_owned();

		let op = self.op().await?;
		let from = self.key();
		op.copy(&from, &to).await.map_err(io::Error::from)?;
		Ok(self.metadata().await?.len)
	}

	fn copy_with_progress<P, A>(&self, to: P, attrs: A) -> io::Result<Receiver<io::Result<u64>>>
	where
		P: AsPath,
		A: Into<yazi_fs::provider::Attrs>,
	{
		let to = UrlBuf::Opendal {
			loc:    LocBuf::<typed_path::UnixPathBuf>::saturated(to.as_path().to_unix_owned()?, SchemeKind::Opendal),
			domain: self.name.intern(),
		};
		let from = self.url.to_owned();

		Ok(crate::provider::copy_with_progress_impl(from, to, attrs.into()))
	}

	async fn create_dir(&self) -> io::Result<()> {
		let op = self.op().await?;
		let mut key = self.key();
		if !key.is_empty() && !key.ends_with('/') {
			key.push('/');
		}

		if let Ok(_) = op.stat(&key).await {
			return Err(io::Error::from(io::ErrorKind::AlreadyExists));
		}

		op.create_dir(&key).await.map_err(io::Error::from)
	}

	async fn hard_link<P>(&self, _to: P) -> io::Result<()>
	where
		P: AsPath,
	{
		Err(io::Error::new(io::ErrorKind::Unsupported, "OpenDAL doesn't support hard_link"))
	}

	async fn metadata(&self) -> io::Result<Cha> {
		let op = self.op().await?;
		let key = self.key();

		let meta = match op.stat(&key).await {
			Ok(m) => m,
			Err(e) if e.kind() == ::opendal::ErrorKind::NotFound && !key.is_empty() && !key.ends_with('/') => {
				op.stat(&(key + "/")).await.map_err(io::Error::from)?
			}
			Err(e) => return Err(io::Error::from(e)),
		};

		Ok(cha_from_meta(&self.url.to_owned(), &meta))
	}

	async fn new<'b>(url: Url<'b>) -> io::Result<Self::Me<'b>> {
		match url {
			Url::Opendal { loc, domain } => {
				let (name, config) = Vfs::service::<&ServiceOpendal>(domain).await?;
				Ok(Self::Me { url, path: loc.as_inner(), name, config })
			}
			_ => Err(io::Error::new(io::ErrorKind::InvalidInput, format!("Not an OpenDAL URL: {url:?}"))),
		}
	}

	async fn read_dir(self) -> io::Result<Self::ReadDir> {
		let op = self.op().await?;
		let mut key = self.key();
		if !key.is_empty() && !key.ends_with('/') {
			key.push('/');
		}

		let entries = op.list_with(&key).await.map_err(io::Error::from)?;

		Ok(Self::ReadDir { dir: Arc::new(self.url.to_owned()), op, entries: entries.into() })
	}

	async fn read_link(&self) -> io::Result<PathBufDyn> {
		Err(io::Error::new(io::ErrorKind::Unsupported, "OpenDAL doesn't support read_link"))
	}

	async fn remove_dir(&self) -> io::Result<()> {
		let op = self.op().await?;
		let mut key = self.key();
		if !key.is_empty() && !key.ends_with('/') {
			key.push('/');
		}
		op.delete(&key).await.map_err(io::Error::from)
	}

	async fn remove_file(&self) -> io::Result<()> {
		self.op().await?.delete(&self.key()).await.map_err(io::Error::from)
	}

	async fn rename<P>(&self, to: P) -> io::Result<()>
	where
		P: AsPath,
	{
		let to = to.as_path().as_unix()?;
		let to = String::from_utf8_lossy(to.as_bytes()).trim_start_matches('/').to_owned();

		self.op().await?.rename(&self.key(), &to).await.map_err(io::Error::from)
	}

	async fn symlink<S, F>(&self, _original: S, _is_dir: F) -> io::Result<()>
	where
		S: AsStrand,
		F: AsyncFnOnce() -> io::Result<bool>,
	{
		Err(io::Error::new(io::ErrorKind::Unsupported, "OpenDAL doesn't support symlink"))
	}

	async fn symlink_metadata(&self) -> io::Result<Cha> { self.metadata().await }

	async fn trash(&self) -> io::Result<()> { self.remove_file().await }

	fn url(&self) -> Url<'_> { self.url }
}
