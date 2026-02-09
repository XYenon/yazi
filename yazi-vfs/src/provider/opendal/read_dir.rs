use std::{collections::VecDeque, io, sync::Arc};

use yazi_fs::provider::{DirReader, FileHolder};
use yazi_shared::{path::PathBufDyn, strand::{StrandBuf, StrandCow}, url::{UrlBuf, UrlLike}};

use super::opendal::cha_from_meta;

pub struct ReadDir {
	pub(super) dir:     Arc<UrlBuf>,
	pub(super) op:      ::opendal::Operator,
	pub(super) entries: VecDeque<::opendal::Entry>,
}

impl DirReader for ReadDir {
	type Entry = DirEntry;

	async fn next(&mut self) -> io::Result<Option<Self::Entry>> {
		Ok(self.entries.pop_front().map(|entry| DirEntry {
			dir: self.dir.clone(),
			op:  self.op.clone(),
			entry,
		}))
	}
}

pub struct DirEntry {
	dir:   Arc<UrlBuf>,
	op:    ::opendal::Operator,
	entry: ::opendal::Entry,
}

impl FileHolder for DirEntry {
	async fn file_type(&self) -> io::Result<yazi_fs::cha::ChaType> {
		Ok(match self.entry.metadata().mode() {
			::opendal::EntryMode::FILE => yazi_fs::cha::ChaType::File,
			::opendal::EntryMode::DIR => yazi_fs::cha::ChaType::Dir,
			::opendal::EntryMode::Unknown => yazi_fs::cha::ChaType::Unknown,
		})
	}

	async fn metadata(&self) -> io::Result<yazi_fs::cha::Cha> {
		let meta = self.op.stat(self.entry.path()).await.map_err(io::Error::from)?;
		Ok(cha_from_meta(&self.url(), &meta))
	}

	fn name(&self) -> StrandCow<'_> {
		let n = self.entry.name().trim_end_matches('/').as_bytes();
		StrandCow::Owned(StrandBuf::Bytes(n.to_vec()))
	}

	fn path(&self) -> PathBufDyn {
		let url = self.url();
		url.loc().to_owned()
	}

	fn url(&self) -> UrlBuf {
		self.dir
			.try_join(self.entry.name().trim_end_matches('/').as_bytes())
			.expect("entry name is a valid component of the OpenDAL URL")
	}
}
