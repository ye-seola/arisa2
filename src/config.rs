use std::{env, fs, path::PathBuf};

use crate::database;

const DEFAULT_BIND: &str = "0.0.0.0:3000";
const DEFAULT_TEMP_DIR: &str = "/sdcard/arisa_temp";
pub const MAX_GRPC_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

pub struct RuntimeConfig {
    pub bind: String,
    pub app_path: String,
    pub referer: String,
    pub database_key: String,
    pub temp_dir: PathBuf,
    pub uid: Option<i32>,
    pub calling_package: String,
    pub db_pull_delay: u64,
    pub exit_on_stdin_close: bool,
}

pub fn load() -> RuntimeConfig {
    let uid = optional_i32("ARISA_UID");
    let app_path = find_app_path(uid).expect("failed to find KakaoTalk app path");
    let referer =
        read_notification_referer(&app_path).expect("failed to read notification referer");

    let preferences = fs::read(format!(
        "{app_path}/files/datastore/Feature_DataStore.pref.preferences_pb"
    ))
    .expect("failed to read KakaoTalk preferences");
    let seed =
        database::crypto_user::extract_db_seed(&preferences).expect("failed to read database seed");
    let database_key = database::crypto_user::kdf(seed).expect("failed to derive database key");

    RuntimeConfig {
        bind: env::var("ARISA_BIND").unwrap_or_else(|_| DEFAULT_BIND.to_string()),
        app_path,
        referer,
        database_key,
        temp_dir: PathBuf::from(DEFAULT_TEMP_DIR),
        uid,
        calling_package: env::var("ARISA_CALLING_PKG")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "com.android.shell".to_string()),
        db_pull_delay: optional_u64("ARISA_DB_PULL_DELAY").unwrap_or(100),
        exit_on_stdin_close: optional_bool("ARISA_EXIT_ON_STDIN_CLOSE").unwrap_or(true),
    }
}

fn optional_i32(name: &str) -> Option<i32> {
    env::var(name).ok().and_then(|value| {
        let value = value.trim();
        (!value.is_empty()).then(|| value.parse().expect("invalid integer environment variable"))
    })
}

fn optional_u64(name: &str) -> Option<u64> {
    env::var(name).ok().and_then(|value| {
        let value = value.trim();
        (!value.is_empty()).then(|| value.parse().expect("invalid integer environment variable"))
    })
}

fn optional_bool(name: &str) -> Option<bool> {
    env::var(name).ok().and_then(|value| {
        let value = value.trim();
        (!value.is_empty()).then(|| value.parse().expect("invalid boolean environment variable"))
    })
}

fn find_app_path(uid: Option<i32>) -> Option<String> {
    let uid = uid.unwrap_or(0);
    [
        "/data/data/com.kakao.talk/".to_string(),
        format!("/data_mirror/data_ce/null/{uid}/com.kakao.talk/"),
        "/data_mirror/data_ce/null/0/com.kakao.talk/".to_string(),
    ]
    .into_iter()
    .map(PathBuf::from)
    .find(|path| path.exists())
    .map(|path| path.to_string_lossy().into_owned())
}

fn read_notification_referer(app_path: &str) -> Option<String> {
    let path = format!("{app_path}/shared_prefs/KakaoTalk.hw.perferences.xml");
    let content = fs::read_to_string(path).ok()?;
    content
        .split(r#"<string name="NotificationReferer">"#)
        .nth(1)
        .and_then(|value| value.split("</string>").next())
        .map(str::to_string)
}
