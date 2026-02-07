use std::{future::Future, io, marker::PhantomData, pin::Pin, task::{Context, Poll}};

use opendal::Operator;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncWrite, ReadBuf};
use yazi_fs::cha::{Cha, ChaKind, ChaMode};

const MAX_INMEM: u64 = 2 * 1024 * 1024;

type FlushFut = Pin<Box<dyn Future<Output = (Option<tokio::fs::File>, io::Result<()>)> + Send>>;
type MigrationFut =
	Pin<Box<dyn Future<Output = Result<Backing, (io::Error, Backing)>> + Send>>;

enum Backing {
	Remote { reader: opendal::Reader },
	Mem { buffer: Vec<u8>, position: u64 },
	Spool { file: Option<tokio::fs::File> },
}

pub struct File {
	op:            Operator,
	path:          String,
	backing:       Backing,
	dirty:         bool,
	mtime:         Option<std::time::SystemTime>,
	flush_fut:     Option<FlushFut>,
	migration_fut: Option<MigrationFut>,
	_not_sync:     PhantomData<std::cell::Cell<()>>,
}

impl File {
	pub async fn open(op: Operator, path: String) -> io::Result<Self> {
		let reader = op.reader(&path).await.map_err(io::Error::other)?;
		let mtime = op.stat(&path).await.ok().and_then(|m| m.last_modified()).and_then(|t| t.try_into().ok());

		Ok(Self {
			op,
			path,
			backing: Backing::Remote { reader },
			dirty: false,
			mtime,
			flush_fut: None,
			migration_fut: None,
			_not_sync: PhantomData,
		})
	}

	pub async fn create(op: Operator, path: String) -> io::Result<Self> {
		Ok(Self {
			op,
			path,
			backing: Backing::Mem { buffer: Vec::new(), position: 0 },
			dirty: true,
			mtime: Some(std::time::SystemTime::now()),
			flush_fut: None,
			migration_fut: None,
			_not_sync: PhantomData,
		})
	}

	pub async fn metadata(&mut self) -> io::Result<Cha> {
		if self.dirty {
			let len = match &self.backing {
				Backing::Remote { reader } => {
					// This case should ideally not happen if dirty is true,
					// but for completeness:
					use tokio::io::AsyncSeekExt;
					let mut reader = reader.clone();
					reader.seek(io::SeekFrom::End(0)).await?
				}
				Backing::Mem { buffer, .. } => buffer.len() as u64,
				Backing::Spool { file } => {
					if let Some(f) = file {
						f.metadata().await?.len()
					} else {
						return Err(io::Error::new(
							io::ErrorKind::ResourceBusy,
							if self.flush_fut.is_some() {
								"file is flushing"
							} else {
								"file is migrating"
							},
						));
					}
				}
			};
			return Ok(Cha {
				kind: ChaKind::empty(),
				mode: ChaMode::T_FILE,
				len,
				atime: None,
				btime: None,
				ctime: None,
				mtime: self.mtime,
				dev: 0,
				uid: 0,
				gid: 0,
				nlink: 0,
			});
		}

		let meta = self.op.stat(&self.path).await.map_err(io::Error::other)?;
		Ok(super::meta_to_cha(&meta))
	}

	fn migrate_to_mem_or_spool(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		if let Some(wrapper) = &mut self.migration_fut {
			match wrapper.as_mut().poll(cx) {
				Poll::Ready(result) => {
					self.migration_fut = None;
					match result {
						Ok(backing) => {
							self.backing = backing;
							return Poll::Ready(Ok(()));
						}
						Err((err, backing)) => {
							self.backing = backing;
							return Poll::Ready(Err(err));
						}
					}
				}
				Poll::Pending => return Poll::Pending,
			}
		}

		match std::mem::replace(&mut self.backing, Backing::Spool { file: None }) {
			Backing::Remote { mut reader } => {
				let op = self.op.clone();
				let path = self.path.clone();
				self.migration_fut = Some(Box::pin(async move {
					let meta = op.stat(&path).await.map_err(io::Error::other)?;
					let size = meta.content_length();

					use tokio::io::AsyncSeekExt;
					let pos = reader.seek(io::SeekFrom::Current(0)).await?;

					if size <= MAX_INMEM {
						let buffer = op.read(&path).await.map_err(io::Error::other)?.to_vec();
						Ok(Backing::Mem { buffer, position: pos })
					} else {
						let tmp = tempfile::tempfile().map_err(io::Error::other)?;
						let mut tmp = tokio::fs::File::from_std(tmp);

						use tokio::io::AsyncReadExt;
						use tokio::io::AsyncWriteExt;
						let mut reader = op.reader(&path).await.map_err(io::Error::other)?;
						tokio::io::copy(&mut reader, &mut tmp).await?;

						tmp.seek(io::SeekFrom::Start(pos)).await?;
						Ok(Backing::Spool { file: Some(tmp) })
					}
				}));
			}
			Backing::Mem { buffer, position } => {
				let tmp = match tempfile::tempfile() {
					Ok(f) => f,
					Err(e) => {
						self.backing = Backing::Mem { buffer, position };
						return Poll::Ready(Err(io::Error::other(e)));
					}
				};
				let mut file = tokio::fs::File::from_std(tmp);
				self.migration_fut = Some(Box::pin(async move {
					use tokio::io::AsyncSeekExt;
					use tokio::io::AsyncWriteExt;
					if let Err(e) = file.write_all(&buffer).await {
						return Err((e, Backing::Mem { buffer, position }));
					}
					if let Err(e) = file.seek(io::SeekFrom::Start(position)).await {
						return Err((e, Backing::Mem { buffer, position }));
					}
					Ok(Backing::Spool { file: Some(file) })
				}));
			}
			other => {
				self.backing = other;
				return Poll::Ready(Ok(()));
			}
		}

		self.migrate_to_mem_or_spool(cx)
	}
}

impl File {
	fn drive_pending_ops(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		if self.migration_fut.is_some() {
			match self.migrate_to_mem_or_spool(cx) {
				Poll::Ready(Ok(())) => {}
				other => return other,
			}
		}

		if let Some(fut) = &mut self.flush_fut {
			match fut.as_mut().poll(cx) {
				Poll::Ready((file, res)) => {
					self.flush_fut = None;
					if let Some(file_handle) = file {
						if let Backing::Spool { file } = &mut self.backing {
							*file = Some(file_handle);
						}
					}
					res?;
					self.dirty = false;
				}
				Poll::Pending => return Poll::Pending,
			}
		}

		Poll::Ready(Ok(()))
	}
}

impl AsyncRead for File {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<io::Result<()>> {
		if matches!(&self.backing, Backing::Spool { file } if file.is_none()) {
			match self.as_mut().drive_pending_ops(cx) {
				Poll::Ready(Ok(())) => {}
				Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
				Poll::Pending => return Poll::Pending,
			}
		}

		match &mut self.backing {
			Backing::Remote { reader } => Pin::new(reader).poll_read(cx, buf),
			Backing::Mem { buffer, position } => {
				let buf_len = buffer.len() as u64;
				if *position >= buf_len {
					return Poll::Ready(Ok(()));
				}
				let pos = *position as usize;
				let remaining = buffer.len() - pos;
				let to_read = remaining.min(buf.remaining());

				if to_read > 0 {
					buf.put_slice(&buffer[pos..pos + to_read]);
					*position += to_read as u64;
				}

				Poll::Ready(Ok(()))
			}
			Backing::Spool { file } => {
				if let Some(file) = file {
					Pin::new(file).poll_read(cx, buf)
				} else {
					Poll::Ready(Err(io::Error::new(
						io::ErrorKind::Other,
						"spool file unavailable after driving pending operations",
					)))
				}
			}
		}
	}
}

impl AsyncWrite for File {
	fn poll_write(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<io::Result<usize>> {
		if self.flush_fut.is_some() || self.migration_fut.is_some() {
			match self.as_mut().drive_pending_ops(cx) {
				Poll::Ready(Ok(())) => {}
				Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
				Poll::Pending => return Poll::Pending,
			}
		}

		match &mut self.backing {
			Backing::Remote { .. } => match self.as_mut().migrate_to_mem_or_spool(cx) {
				Poll::Ready(Ok(())) => self.poll_write(cx, buf),
				Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
				Poll::Pending => Poll::Pending,
			},
			Backing::Mem { position, .. } => {
				let write_len = buf.len() as u64;
				let end_pos = position.checked_add(write_len).unwrap_or(u64::MAX);
				if end_pos > MAX_INMEM {
					match self.as_mut().migrate_to_mem_or_spool(cx) {
						Poll::Ready(Ok(())) => self.poll_write(cx, buf),
						Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
						Poll::Pending => Poll::Pending,
					}
				} else {
					let Backing::Mem { buffer, position } = &mut self.backing else { unreachable!() };
					let pos = *position as usize;
					let end = pos.checked_add(buf.len()).ok_or_else(|| {
						io::Error::new(io::ErrorKind::InvalidInput, "write position overflow")
					})?;

					if end > buffer.len() {
						buffer.resize(end, 0);
					}
					buffer[pos..end].copy_from_slice(buf);
					*position += buf.len() as u64;
					self.dirty = true;
					self.mtime = Some(std::time::SystemTime::now());

					Poll::Ready(Ok(buf.len()))
				}
			}
			Backing::Spool { file } => {
				if let Some(file) = file {
					let result = Pin::new(file).poll_write(cx, buf);
					if matches!(&result, Poll::Ready(Ok(_))) {
						self.dirty = true;
						self.mtime = Some(std::time::SystemTime::now());
					}
					result
				} else {
					Poll::Ready(Err(io::Error::new(
						io::ErrorKind::Other,
						"spool file unavailable after driving pending operations",
					)))
				}
			}
		}
	}

	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		// Wait for any existing flush/migration to complete first
		if self.flush_fut.is_some() || self.migration_fut.is_some() {
			match self.as_mut().drive_pending_ops(cx) {
				Poll::Ready(Ok(())) => {}
				Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
				Poll::Pending => return Poll::Pending,
			}
		}

		if self.flush_fut.is_none() {
			if !self.dirty {
				return Poll::Ready(Ok(()));
			}

			let op = self.op.clone();
			let path = self.path.clone();

			match &mut self.backing {
				Backing::Remote { .. } => unreachable!(),
				Backing::Mem { buffer, .. } => {
					let buf_clone = buffer.clone();
					self.flush_fut = Some(Box::pin(async move {
						match op.write(&path, buf_clone).await {
							Ok(_) => (None, Ok(())),
							Err(e) => (None, Err(io::Error::other(e))),
						}
					}));
				}
				Backing::Spool { file } => {
					let Some(file_handle) = file.take() else {
						return Poll::Ready(Err(io::Error::new(
							io::ErrorKind::ResourceBusy,
							"spool file unavailable",
						)));
					};
					let mut file_handle = file_handle;
					self.flush_fut = Some(Box::pin(async move {
						use tokio::io::AsyncReadExt;
						use tokio::io::AsyncSeekExt;
						use tokio::io::AsyncWriteExt;

						let saved_pos = match file_handle.seek(io::SeekFrom::Current(0)).await {
							Ok(p) => p,
							Err(e) => return (Some(file_handle), Err(e)),
						};

						if let Err(e) = file_handle.seek(io::SeekFrom::Start(0)).await {
							return (Some(file_handle), Err(e));
						}

						let mut writer = match op.writer(&path).await {
							Ok(w) => w,
							Err(e) => return (Some(file_handle), Err(io::Error::other(e))),
						};

						let mut buf = [0u8; 64 * 1024];
						loop {
							match file_handle.read(&mut buf).await {
								Ok(0) => break,
								Ok(n) => {
									if let Err(e) =
										writer.write(bytes::Bytes::copy_from_slice(&buf[..n])).await
									{
										let _ =
											file_handle.seek(io::SeekFrom::Start(saved_pos)).await;
										return (Some(file_handle), Err(io::Error::other(e)));
									}
								}
								Err(e) => {
									let _ = file_handle.seek(io::SeekFrom::Start(saved_pos)).await;
									return (Some(file_handle), Err(e));
								}
							}
						}

						if let Err(e) = writer.close().await {
							let _ = file_handle.seek(io::SeekFrom::Start(saved_pos)).await;
							return (Some(file_handle), Err(io::Error::other(e)));
						}

						if let Err(e) = file_handle.seek(io::SeekFrom::Start(saved_pos)).await {
							return (Some(file_handle), Err(e));
						}

						(Some(file_handle), Ok(()))
					}));
				}
			}
		}

		let wrapper = self.flush_fut.as_mut().unwrap();
		match wrapper.as_mut().poll(cx) {
			Poll::Ready((file, res)) => {
				self.flush_fut = None;

				if let Some(file_handle) = file {
					if let Backing::Spool { file } = &mut self.backing {
						*file = Some(file_handle);
					}
				}

				res?;
				self.dirty = false;
				Poll::Ready(Ok(()))
			}
			Poll::Pending => Poll::Pending,
		}
	}

	fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		self.poll_flush(cx)
	}
}

impl AsyncSeek for File {
	fn start_seek(mut self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
		match &mut self.backing {
			Backing::Remote { reader } => Pin::new(reader).start_seek(position),
			Backing::Mem { buffer, position: pos } => {
				let new_pos = match position {
					io::SeekFrom::Start(p) => i64::try_from(p).map_err(|_| {
						io::Error::new(io::ErrorKind::InvalidInput, "seek position overflow")
					})?,
					io::SeekFrom::End(offset) => {
						let len = i64::try_from(buffer.len()).map_err(|_| {
							io::Error::new(io::ErrorKind::InvalidInput, "seek position overflow")
						})?;
						len.checked_add(offset).ok_or_else(|| {
							io::Error::new(io::ErrorKind::InvalidInput, "seek position overflow")
						})?
					}
					io::SeekFrom::Current(offset) => {
						let cur = i64::try_from(*pos).map_err(|_| {
							io::Error::new(io::ErrorKind::InvalidInput, "seek position overflow")
						})?;
						cur.checked_add(offset).ok_or_else(|| {
							io::Error::new(io::ErrorKind::InvalidInput, "seek position overflow")
						})?
					}
				};

				if new_pos < 0 {
					return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid seek"));
				}

				*pos = new_pos as u64;
				Ok(())
			}
			Backing::Spool { file } => {
				if let Some(file) = file {
					Pin::new(file).start_seek(position)
				} else {
					Err(io::Error::new(
						io::ErrorKind::ResourceBusy,
						if self.flush_fut.is_some() {
							"file is flushing"
						} else {
							"file is migrating"
						},
					))
				}
			}
		}
	}

	fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
		if matches!(&self.backing, Backing::Spool { file } if file.is_none()) {
			match self.as_mut().drive_pending_ops(cx) {
				Poll::Ready(Ok(())) => {}
				Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
				Poll::Pending => return Poll::Pending,
			}
		}

		match &mut self.backing {
			Backing::Remote { reader } => Pin::new(reader).poll_complete(cx),
			Backing::Mem { position, .. } => Poll::Ready(Ok(*position)),
			Backing::Spool { file } => {
				if let Some(file) = file {
					Pin::new(file).poll_complete(cx)
				} else {
					Poll::Ready(Err(io::Error::new(
						io::ErrorKind::Other,
						"spool file unavailable after driving pending operations",
					)))
				}
			}
		}
	}
}
