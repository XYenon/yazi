use std::{collections::{HashMap, VecDeque}, io, sync::Arc};

use futures::TryStreamExt;
use yazi_fs::provider::{DirReader, FileHolder};
use yazi_shared::{path::PathBufDyn, strand::{StrandBuf, StrandCow}, url::{UrlBuf, UrlLike}};

use super::opendal::cha_from_meta;

#[derive(Clone)]
pub(super) struct ListedEntry {
	pub(super) path: String,
	pub(super) name: String,
	pub(super) meta: ::opendal::Metadata,
	pub(super) synth: bool,
}

pub(super) async fn normalize_entries(
	prefix: &str,
	mut lister: ::opendal::Lister,
) -> io::Result<VecDeque<ListedEntry>> {
	let mut out: Vec<ListedEntry> = Vec::new();
	let mut index: HashMap<String, usize> = HashMap::new();

	while let Some(entry) = lister.try_next().await.map_err(io::Error::from)? {
		let (path, meta) = entry.into_parts();

		if !prefix.is_empty() && !path.starts_with(prefix) {
			continue;
		}

		let rest = if prefix.is_empty() {
			path.as_str()
		} else {
			path.strip_prefix(prefix).unwrap_or("")
		};

		// Some object stores may return the "directory marker object" for the directory itself.
		// Skip it so we don't show `dir/dir` when listing `dir/`.
		if rest.is_empty() {
			continue;
		}

		let (name, synth) = match rest.split_once('/') {
			Some((head, _)) if !head.is_empty() => (format!("{head}/"), true),
			Some(_) => continue,
			None => (rest.to_owned(), false),
		};

		let full_path = if prefix.is_empty() { name.clone() } else { format!("{prefix}{name}") };
		let meta = if synth { ::opendal::Metadata::new(::opendal::EntryMode::DIR) } else { meta };

		let new = ListedEntry { path: full_path.clone(), name, meta, synth };

		match index.get(&full_path).copied() {
			Some(i) if out[i].synth && !new.synth => out[i] = new,
			Some(_) => {}
			None => {
				index.insert(full_path, out.len());
				out.push(new);
			}
		}
	}

	Ok(out.into())
}

pub struct ReadDir {
	pub(super) dir:     Arc<UrlBuf>,
	pub(super) op:      ::opendal::Operator,
	pub(super) entries: VecDeque<ListedEntry>,
}

impl DirReader for ReadDir {
	type Entry = DirEntry;

	async fn next(&mut self) -> io::Result<Option<Self::Entry>> {
		while let Some(entry) = self.entries.pop_front() {
			// Some backends (or buggy S3-compatible services) may return an entry that maps to the
			// operator root itself, producing an empty name. Skip it to avoid panics downstream.
			if entry.name.trim_end_matches('/').is_empty() {
				continue;
			}

			return Ok(Some(DirEntry { dir: self.dir.clone(), op: self.op.clone(), entry }));
		}
		Ok(None)
	}
}

pub struct DirEntry {
	dir:   Arc<UrlBuf>,
	op:    ::opendal::Operator,
	entry: ListedEntry,
}

impl FileHolder for DirEntry {
	async fn file_type(&self) -> io::Result<yazi_fs::cha::ChaType> {
		Ok(match self.entry.meta.mode() {
			::opendal::EntryMode::FILE => yazi_fs::cha::ChaType::File,
			::opendal::EntryMode::DIR => yazi_fs::cha::ChaType::Dir,
			::opendal::EntryMode::Unknown => yazi_fs::cha::ChaType::Unknown,
		})
	}

	async fn metadata(&self) -> io::Result<yazi_fs::cha::Cha> {
		if self.entry.synth {
			return Ok(cha_from_meta(&self.url(), &self.entry.meta));
		}

		let meta = self.op.stat(&self.entry.path).await.map_err(io::Error::from)?;
		Ok(cha_from_meta(&self.url(), &meta))
	}

	fn name(&self) -> StrandCow<'_> {
		let n = self.entry.name.trim_end_matches('/').as_bytes();
		StrandCow::Owned(StrandBuf::Bytes(n.to_vec()))
	}

	fn path(&self) -> PathBufDyn {
		let url = self.url();
		url.loc().to_owned()
	}

	fn url(&self) -> UrlBuf {
		self.dir
			.try_join(self.entry.name.trim_end_matches('/').as_bytes())
			.expect("entry name is a valid component of the OpenDAL URL")
	}
}
