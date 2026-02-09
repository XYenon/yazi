yazi_macro::mod_flat!(absolute file gate opendal read_dir);

use std::{collections::HashMap, io, path::PathBuf, sync::Arc};

use parking_lot::Mutex;
use yazi_shared::RoCell;

pub(super) static OPS: RoCell<Mutex<HashMap<&'static str, ::opendal::Operator>>> = RoCell::new();
pub(super) static SESSIONS: RoCell<Mutex<HashMap<PathBuf, Arc<file::WriteSession>>>> = RoCell::new();

pub(super) fn key_from_unix(path: &typed_path::UnixPath) -> io::Result<String> {
	let s = std::str::from_utf8(path.as_bytes())
		.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "OpenDAL path must be valid UTF-8"))?;
	Ok(s.trim_start_matches('/').to_owned())
}

pub(super) fn init() {
	OPS.init(Default::default());
	SESSIONS.init(Default::default());
}
