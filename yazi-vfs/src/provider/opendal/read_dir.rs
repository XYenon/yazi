use std::io;

use futures::TryStreamExt;
use opendal::Operator;
use yazi_fs::provider::DirReader;

use super::DirEntry;

pub struct ReadDir {
	lister: opendal::Lister,
	domain: String,
}

impl ReadDir {
	pub async fn new(op: Operator, path: String, domain: String) -> io::Result<Self> {
		let lister = op
			.lister_with(&path)
			.metakey(
				opendal::Metakey::Mode
					| opendal::Metakey::ContentLength
					| opendal::Metakey::LastModified,
			)
			.await
			.map_err(io::Error::other)?;

		Ok(Self { lister, domain })
	}
}

impl DirReader for ReadDir {
	type Entry = DirEntry;

	async fn next(&mut self) -> io::Result<Option<Self::Entry>> {
		let Some(entry) = self.lister.try_next().await.map_err(io::Error::other)? else {
			return Ok(None);
		};

		let mut name = entry.name().to_string();
		if name.ends_with('/') {
			name.pop();
		}

		let path = entry.path().to_string();
		let meta = entry.metadata().clone();

		Ok(Some(DirEntry {
			name,
			path,
			meta,
			domain: self.domain.clone(),
		}))
	}
}
