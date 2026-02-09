yazi_macro::mod_flat!(absolute file gate opendal read_dir);

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use parking_lot::Mutex;
use yazi_shared::RoCell;

pub(super) static OPS: RoCell<Mutex<HashMap<&'static str, ::opendal::Operator>>> = RoCell::new();
pub(super) static SESSIONS: RoCell<Mutex<HashMap<PathBuf, Arc<file::WriteSession>>>> = RoCell::new();

pub(super) fn init() {
	OPS.init(Default::default());
	SESSIONS.init(Default::default());
}
