use std::time::Duration;

use tokio::{pin, task::JoinHandle};
use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};
use tokio_util::sync::CancellationToken;
use yazi_adapter::ADAPTOR;
use yazi_config::PLUGIN;
use yazi_fs::Files;
use yazi_plugin::{external::Highlighter, isolate, utils::PreviewLock};
use yazi_shared::{MIME_DIR, fs::{Cha, File, FilesOp, Url}};

#[derive(Default)]
pub struct Preview {
	pub lock: Option<PreviewLock>,
	pub skip: usize,

	previewer_ct:  Option<CancellationToken>,
	folder_loader: Option<JoinHandle<()>>,
}

impl Preview {
	pub fn go(&mut self, file: File, mime: &str, force: bool) {
		if !force && self.content_unchanged(&file.url, file.cha) {
			return;
		}

		let Some(previewer) = PLUGIN.previewer(&file.url, mime) else {
			self.reset();
			return;
		};

		self.abort();
		if previewer.sync {
			isolate::peek_sync(&previewer.run, file, self.skip);
		} else {
			self.previewer_ct = Some(isolate::peek(&previewer.run, file, self.skip));
		}
	}

	pub fn go_folder(&mut self, file: File, dir: Option<Cha>, force: bool) {
		let (cha, cwd) = (file.cha, file.url_owned());
		self.go(file, MIME_DIR, force);

		if self.content_unchanged(&cwd, cha) {
			return;
		}

		self.folder_loader.take().map(|h| h.abort());
		self.folder_loader = Some(tokio::spawn(async move {
			let Some(new) = Files::assert_stale(&cwd, dir.unwrap_or(Cha::dummy())).await else {
				return;
			};
			let Ok(rx) = Files::from_dir(&cwd).await else { return };

			let stream =
				UnboundedReceiverStream::new(rx).chunks_timeout(50000, Duration::from_millis(500));
			pin!(stream);

			let ticket = FilesOp::prepare(&cwd);
			while let Some(chunk) = stream.next().await {
				FilesOp::Part(cwd.clone(), chunk, ticket).emit();
			}
			FilesOp::Done(cwd, new, ticket).emit();
		}));
	}

	#[inline]
	pub fn abort(&mut self) {
		self.previewer_ct.take().map(|ct| ct.cancel());
		Highlighter::abort();
	}

	#[inline]
	pub fn reset(&mut self) -> bool {
		self.abort();
		ADAPTOR.image_hide().ok();
		self.lock.take().is_some()
	}

	#[inline]
	pub fn reset_image(&mut self) {
		self.abort();
		ADAPTOR.image_hide().ok();
	}

	#[inline]
	pub fn same_url(&self, url: &Url) -> bool {
		matches!(self.lock, Some(ref lock) if lock.url == *url)
	}

	#[inline]
	fn content_unchanged(&self, url: &Url, cha: Cha) -> bool {
		match &self.lock {
			Some(l) => *url == l.url && self.skip == l.skip && cha.hits(l.cha),
			None => false,
		}
	}
}
