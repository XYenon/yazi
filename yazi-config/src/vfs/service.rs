use std::{io, mem, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::normalize_path;

#[derive(Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Service {
	Sftp(ServiceSftp),
	OpenDal(ServiceOpenDal),
}

impl TryFrom<&'static Service> for &'static ServiceSftp {
	type Error = &'static str;

	fn try_from(value: &'static Service) -> Result<Self, Self::Error> {
		match value {
			Service::Sftp(p) => Ok(p),
			_ => Err("not an sftp service"),
		}
	}
}

impl TryFrom<&'static Service> for &'static ServiceOpenDal {
	type Error = &'static str;

	fn try_from(value: &'static Service) -> Result<Self, Self::Error> {
		match value {
			Service::OpenDal(p) => Ok(p),
			_ => Err("not an opendal service"),
		}
	}
}

impl Service {
	pub(super) fn reshape(&mut self) -> io::Result<()> {
		match self {
			Self::Sftp(p) => p.reshape(),
			Self::OpenDal(_) => Ok(()),
		}
	}
}

// --- SFTP
#[derive(Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ServiceSftp {
	pub host:           String,
	pub user:           String,
	pub port:           u16,
	pub password:       Option<String>,
	#[serde(default)]
	pub key_file:       PathBuf,
	pub key_passphrase: Option<String>,
	#[serde(default)]
	pub identity_agent: PathBuf,
}

// --- OpenDal
#[derive(Deserialize, PartialEq, Eq, Serialize)]
pub struct ServiceOpenDal {
	pub backend: String,
	#[serde(default)]
	pub options: std::collections::HashMap<String, String>,
}

impl ServiceSftp {
	fn reshape(&mut self) -> io::Result<()> {
		if !self.key_file.as_os_str().is_empty() {
			self.key_file = normalize_path(mem::take(&mut self.key_file))
				.ok_or_else(|| io::Error::other("key_file must be either empty or an absolute path"))?;
		}

		self.identity_agent = if self.identity_agent.as_os_str().is_empty() {
			std::env::var_os("SSH_AUTH_SOCK")
				.map(PathBuf::from)
				.filter(|p| p.is_absolute())
				.unwrap_or_default()
		} else {
			normalize_path(mem::take(&mut self.identity_agent)).ok_or_else(|| {
				io::Error::other("identity_agent must be either empty or an absolute path")
			})?
		};

		Ok(())
	}
}
