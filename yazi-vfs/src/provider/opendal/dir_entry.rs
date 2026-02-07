use std::io;

use typed_path::UnixPathBuf;
use yazi_fs::{cha::{Cha, ChaType}, provider::FileHolder};
use yazi_shared::{loc::LocBuf, path::PathBufDyn, strand::StrandCow, url::UrlBuf};

pub struct DirEntry {
	pub(super) name: String,
	pub(super) path: String,
	pub(super) meta: opendal::Metadata,
	pub(super) domain: String,
}

impl FileHolder for DirEntry {
	async fn file_type(&self) -> io::Result<ChaType> {
		Ok(self.cha().mode.into())
	}

	async fn metadata(&self) -> io::Result<Cha> {
		Ok(self.cha())
	}

	fn name(&self) -> StrandCow<'_> {
		self.name.as_str().into()
	}

	fn path(&self) -> PathBufDyn {
		PathBufDyn::Unix(self.path.as_str().into())
	}

	fn url(&self) -> UrlBuf {
		UrlBuf::OpenDal {
			loc:    LocBuf::from(UnixPathBuf::from(self.path.as_str())),
			domain: yazi_shared::pool::Pool::<str>::intern(&self.domain),
		}
	}
}

impl DirEntry {
	pub fn cha(&self) -> Cha {
		super::meta_to_cha(&self.meta)
	}
}
