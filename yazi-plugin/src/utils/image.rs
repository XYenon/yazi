use mlua::{Function, IntoLuaMulti, Lua, Value};
use yazi_adapter::{ADAPTOR, Image};
use yazi_binding::{Error, ImageInfo, UrlRef, elements::Rect};
use yazi_fs::FsUrl;
use yazi_shared::url::{AsUrl, UrlLike};

use super::Utils;

async fn ensure_cached(url: &UrlRef) -> mlua::Result<()> {
	if !url.as_url().kind().is_local() {
		drop(
			yazi_vfs::provider::open(url.as_url())
				.await
				.map_err(mlua::Error::external)?,
		);
	}
	Ok(())
}

impl Utils {
	pub(super) fn image_info(lua: &Lua) -> mlua::Result<Function> {
		lua.create_async_function(|lua, url: UrlRef| async move {
			ensure_cached(&url).await?;
			let path = url.as_url().unified_path().into_owned();
			match yazi_adapter::ImageInfo::new(path).await {
				Ok(info) => ImageInfo::from(info).into_lua_multi(&lua),
				Err(e) => (Value::Nil, Error::custom(e.to_string())).into_lua_multi(&lua),
			}
		})
	}

	pub(super) fn image_show(lua: &Lua) -> mlua::Result<Function> {
		lua.create_async_function(|lua, (url, rect): (UrlRef, Rect)| async move {
			ensure_cached(&url).await?;
			let path = url.as_url().unified_path();
			match ADAPTOR.get().image_show(path, *rect).await {
				Ok(area) => Rect::from(area).into_lua_multi(&lua),
				Err(e) => (Value::Nil, Error::custom(e.to_string())).into_lua_multi(&lua),
			}
		})
	}

	pub(super) fn image_precache(lua: &Lua) -> mlua::Result<Function> {
		lua.create_async_function(|lua, (src, dist): (UrlRef, UrlRef)| async move {
			let Some(dist) = dist.as_local() else {
				return (Value::Nil, Error::custom("Destination must be a local path"))
					.into_lua_multi(&lua);
			};
			ensure_cached(&src).await?;
			let src = src.as_url().unified_path().into_owned();
			match Image::precache(src, dist).await {
				Ok(()) => true.into_lua_multi(&lua),
				Err(e) => (false, Error::custom(e.to_string())).into_lua_multi(&lua),
			}
		})
	}
}
