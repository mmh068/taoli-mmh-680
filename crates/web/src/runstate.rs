//! 进程运行标记：用来发现「上次异常崩掉」。
//!
//! 进程被 SIGKILL / OOM / panic 打死时，自己已经发不出告警。做法是：
//! 1. 服务真正起来后写一份「还在跑」的标记（`clean = false`）；
//! 2. 优雅停机发完最后一条告警后再改成 `clean = true`；
//! 3. 下次启动若看到标记仍是脏的，就补推一条「上次未正常退出」；
//! 4. panic 钩子尽量把原因写进标记，重启告警能带上异常信息。
//!
//! 标记路径默认 `arb-web.run.json`，可用 `ARB_WEB_RUN_STATE` 覆盖。

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

static PATH: OnceLock<PathBuf> = OnceLock::new();

/// 落盘的运行标记。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunState {
    pub pid: u32,
    pub started_at: DateTime<Utc>,
    pub clean: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    /// panic 钩子写进来的最后一次异常摘要；优雅停机会清掉。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl RunState {
    fn fresh() -> Self {
        Self {
            pid: std::process::id(),
            started_at: Utc::now(),
            clean: false,
            hostname: hostname(),
            last_error: None,
        }
    }

    /// 给告警用的「上次异常退出」说明。
    pub fn unclean_message(&self) -> String {
        let when = self.started_at.format("%Y-%m-%d %H:%M:%S UTC");
        let host = self
            .hostname
            .as_deref()
            .map(|name| format!(" @ {name}"))
            .unwrap_or_default();
        let mut text = format!(
            "🚨 arb-web 上次未正常退出：pid {}{host}，启动于 {when}",
            self.pid
        );
        if let Some(error) = self
            .last_error
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            text.push_str("\n原因：");
            text.push_str(error);
        } else {
            text.push_str("\n原因：进程没走到优雅停机（崩溃 / 被强杀 / 断电 / OOM 等）");
        }
        text.push_str("。请先看持仓页对账与未完结操作。");
        text
    }
}

/// 解析标记路径并安装 panic 钩子。启动早期调用一次即可。
pub fn init() -> PathBuf {
    let path = path_from_env();
    let _ = PATH.set(path.clone());
    install_panic_hook(path.clone());
    path
}

fn path_from_env() -> PathBuf {
    std::env::var("ARB_WEB_RUN_STATE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("arb-web.run.json"))
}

fn path() -> PathBuf {
    PATH.get().cloned().unwrap_or_else(path_from_env)
}

/// 读出上一次留下的标记；没有或读不懂就当没有。
pub fn load_prior() -> Option<RunState> {
    load(&path())
}

fn load(path: &Path) -> Option<RunState> {
    match std::fs::read_to_string(path) {
        Ok(text) => match serde_json::from_str::<RunState>(&text) {
            Ok(state) => Some(state),
            Err(error) => {
                warn!(%error, path = %path.display(), "运行标记读不懂，忽略");
                None
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            warn!(%error, path = %path.display(), "运行标记读不了，忽略");
            None
        }
    }
}

/// 服务已起来：写下「还在跑」标记。之后任何非优雅退出都会在下次启动被发现。
pub fn mark_running() {
    let state = RunState::fresh();
    if let Err(error) = save(&path(), &state) {
        warn!(%error, "写运行标记失败：异常退出将无法在下次启动时告警");
        return;
    }
    info!(path = %path().display(), pid = state.pid, "已写入运行标记");
}

/// 优雅停机完成（含「强制退出但已告警」）：标成干净，避免重启误报崩溃。
pub fn mark_clean() {
    let path = path();
    let mut state = load(&path).unwrap_or_else(RunState::fresh);
    state.clean = true;
    state.last_error = None;
    if let Err(error) = save(&path, &state) {
        warn!(%error, "标记干净退出失败：下次启动可能误报异常退出");
    }
}

fn save(path: &Path, state: &RunState) -> std::io::Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = {
        let mut name = path.as_os_str().to_owned();
        name.push(".tmp");
        PathBuf::from(name)
    };
    let body = serde_json::to_vec_pretty(state).map_err(std::io::Error::other)?;
    let mut file = std::fs::File::create(&tmp)?;
    file.write_all(&body)?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)
}

fn install_panic_hook(path: PathBuf) {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let message = panic_message(info);
        // 钩子里不能依赖 tokio；只尽力落盘，告警留给下次启动。
        let mut state = load(&path).unwrap_or_else(RunState::fresh);
        state.clean = false;
        state.last_error = Some(truncate_error(message));
        let _ = save(&path, &state);
        previous(info);
    }));
}

fn panic_message(info: &std::panic::PanicHookInfo<'_>) -> String {
    let location = info
        .location()
        .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
        .unwrap_or_else(|| "unknown".into());
    let payload = if let Some(text) = info.payload().downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = info.payload().downcast_ref::<String>() {
        text.clone()
    } else {
        "non-string panic payload".into()
    };
    format!("panic at {location}: {payload}")
}

fn truncate_error(text: String) -> String {
    const MAX: usize = 500;
    if text.chars().count() <= MAX {
        return text;
    }
    let mut cut: String = text.chars().take(MAX).collect();
    cut.push('…');
    cut
}

fn hostname() -> Option<String> {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_path(label: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("arb-web-runstate-{label}-{n}.json"))
    }

    #[test]
    fn unclean_prior_run_is_detected_until_marked_clean() {
        let path = temp_path("unclean");
        let _ = std::fs::remove_file(&path);
        save(
            &path,
            &RunState {
                pid: 42,
                started_at: Utc::now(),
                clean: false,
                hostname: Some("box".into()),
                last_error: Some("panic at main.rs:1:1: boom".into()),
            },
        )
        .unwrap();

        let prior = load(&path).unwrap();
        assert!(!prior.clean);
        let message = prior.unclean_message();
        assert!(message.contains("上次未正常退出"), "{message}");
        assert!(message.contains("boom"), "{message}");

        let mut clean = prior;
        clean.clean = true;
        clean.last_error = None;
        save(&path, &clean).unwrap();
        assert!(load(&path).unwrap().clean);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn missing_file_means_no_prior_run() {
        let path = temp_path("missing");
        let _ = std::fs::remove_file(&path);
        assert!(load(&path).is_none());
    }

    #[test]
    fn panic_record_keeps_marker_dirty() {
        let path = temp_path("panic");
        let _ = std::fs::remove_file(&path);
        save(&path, &RunState::fresh()).unwrap();
        let mut state = load(&path).unwrap();
        state.clean = false;
        state.last_error = Some(truncate_error("panic at x:1:1: boom".into()));
        save(&path, &state).unwrap();
        let loaded = load(&path).unwrap();
        assert!(!loaded.clean);
        assert_eq!(loaded.last_error.as_deref(), Some("panic at x:1:1: boom"));
        let _ = std::fs::remove_file(&path);
    }
}
