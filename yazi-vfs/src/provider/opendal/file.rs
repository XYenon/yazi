use std::{future::Future, io, path::PathBuf, pin::Pin, sync::{Arc, atomic::{AtomicUsize, Ordering}}, task::{Context, Poll}};

use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

pub struct WriteSession {
	pub op:         ::opendal::Operator,
	pub key:        String,
	pub cache_path: PathBuf,
	pub writers:    AtomicUsize,
}

impl WriteSession {
	pub fn new(op: ::opendal::Operator, key: String, cache_path: PathBuf) -> Self {
		Self { op, key, cache_path, writers: AtomicUsize::new(0) }
	}
}

pub struct File {
	inner:  tokio::fs::File,
	session: Option<Arc<WriteSession>>,
	upload: Mutex<Option<Pin<Box<dyn Future<Output = io::Result<()>> + Send>>>>,
}

impl File {
	pub fn new(inner: tokio::fs::File, session: Option<Arc<WriteSession>>) -> Self {
		Self { inner, session, upload: Mutex::new(None) }
	}

	#[inline]
	pub async fn metadata(&self) -> io::Result<std::fs::Metadata> { self.inner.metadata().await }

	#[inline]
	pub async fn set_len(&self, size: u64) -> io::Result<()> { self.inner.set_len(size).await }

	#[inline]
	pub async fn try_clone(&self) -> io::Result<tokio::fs::File> { self.inner.try_clone().await }

	async fn upload(session: Arc<WriteSession>) -> io::Result<()> {
		use tokio::io::AsyncReadExt;

		if session.key.is_empty() {
			return Err(io::Error::other("OpenDAL upload path is empty"));
		}

		let mut r = tokio::fs::File::open(&session.cache_path).await?;
		let mut w = session.op.writer(&session.key).await.map_err(io::Error::from)?;

		let mut buf = vec![0u8; 8 * 1024 * 1024];
		loop {
			let n = r.read(&mut buf).await?;
			if n == 0 {
				break;
			}
			w.write(buf[..n].to_vec()).await.map_err(io::Error::from)?;
		}

		w.close().await.map_err(io::Error::from)?;
		Ok(())
	}
}

impl Drop for File {
	fn drop(&mut self) {
		let Some(session) = self.session.take() else { return };
		if session.writers.fetch_sub(1, Ordering::SeqCst) != 1 {
			return;
		}

		let key = session.cache_path.clone();
		super::SESSIONS.lock().remove(&key);

		if let Ok(handle) = tokio::runtime::Handle::try_current() {
			handle.spawn(async move {
				let _ = Self::upload(session).await;
			});
		}
	}
}

impl AsyncRead for File {
	#[inline]
	fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
		unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll_read(cx, buf)
	}
}

impl AsyncSeek for File {
	#[inline]
	fn start_seek(self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
		unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.start_seek(position)
	}

	#[inline]
	fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
		unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll_complete(cx)
	}
}

impl AsyncWrite for File {
	#[inline]
	fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
		unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll_write(cx, buf)
	}

	#[inline]
	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll_flush(cx)
	}

	fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		let inner_poll = unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.inner) }.poll_shutdown(cx);
		if !matches!(inner_poll, Poll::Ready(Ok(()))) {
			return inner_poll;
		}

		{
			let mut upload = self.upload.lock();
			if let Some(fut) = upload.as_mut() {
				return match fut.as_mut().poll(cx) {
					Poll::Ready(r) => {
						*upload = None;
						Poll::Ready(r)
					}
					Poll::Pending => Poll::Pending,
				};
			}
		}

		let Some(session) = self.session.take() else {
			return Poll::Ready(Ok(()));
		};

		if session.writers.fetch_sub(1, Ordering::SeqCst) != 1 {
			return Poll::Ready(Ok(()));
		}

		let key = session.cache_path.clone();
		super::SESSIONS.lock().remove(&key);

		let mut upload = self.upload.lock();
		*upload = Some(Box::pin(Self::upload(session)));
		match upload.as_mut().unwrap().as_mut().poll(cx) {
			Poll::Ready(r) => {
				*upload = None;
				Poll::Ready(r)
			}
			Poll::Pending => Poll::Pending,
		}
	}

	#[inline]
	fn poll_write_vectored(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		bufs: &[io::IoSlice<'_>],
	) -> Poll<io::Result<usize>> {
		unsafe { self.map_unchecked_mut(|s| &mut s.inner) }.poll_write_vectored(cx, bufs)
	}

	#[inline]
	fn is_write_vectored(&self) -> bool { self.inner.is_write_vectored() }
}
