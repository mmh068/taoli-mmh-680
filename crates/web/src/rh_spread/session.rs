//! 交易时段：股票 / 指数 / 商品永续的价差在盘中、盘后、周末往往不是同一个水平，
//! 「正常价差」要按时段分开统计。加密币 24/7，不分。
//!
//! 优先用 Arcus `/v1/markets` 的 `isOutsideRth`（它知道节假日与提前收盘）；拿不到时按
//! 纽约时间 周一至周五 9:30–16:00 推算（不含节假日）。

use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Timelike, Utc, Weekday};
use serde::{Deserialize, Serialize};

use super::history::{BREAK_MARK_MINUTES, CRYPTO_BREAK_HOURS};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Session {
    /// 美股常规交易时段。
    Rth,
    /// 工作日盘前 / 盘后 / 夜盘。
    Off,
    /// 纽约时间周六、周日。
    Weekend,
    /// 加密币：不分时段。
    All,
}

impl Session {
    pub fn label(self) -> &'static str {
        match self {
            Session::Rth => "盘中",
            Session::Off => "盘后",
            Session::Weekend => "周末",
            Session::All => "全天",
        }
    }
}

/// 第 `nth` 个星期日（`nth` 从 1 开始）。
fn nth_sunday(year: i32, month: u32, nth: u32) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(year, month, 1).expect("合法月份");
    let offset = (7 - first.weekday().num_days_from_sunday()) % 7;
    first + Duration::days(i64::from(offset + 7 * (nth - 1)))
}

/// 纽约相对 UTC 的小时偏移：夏令时 3 月第二个周日 2:00 起、11 月第一个周日 2:00 止。
pub fn new_york_offset_hours(at: DateTime<Utc>) -> i64 {
    let year = at.year();
    // 当地 2:00 = UTC 7:00（标准时间时）开始；当地 2:00 = UTC 6:00（夏令时时）结束。
    let start = Utc.from_utc_datetime(&nth_sunday(year, 3, 2).and_hms_opt(7, 0, 0).expect("时刻"));
    let end = Utc.from_utc_datetime(&nth_sunday(year, 11, 1).and_hms_opt(6, 0, 0).expect("时刻"));
    if at >= start && at < end { -4 } else { -5 }
}

fn local_of(at: DateTime<Utc>) -> DateTime<Utc> {
    at + Duration::hours(new_york_offset_hours(at))
}

fn utc_of(local: DateTime<Utc>, at: DateTime<Utc>) -> DateTime<Utc> {
    local - Duration::hours(new_york_offset_hours(at))
}

/// 包含 `at` 的这一段时段从什么时候开始。
///
/// 盘中从 9:30 起。盘后从当天 16:00 起，到下一次 9:30，过夜不算新的一段。
/// 周末从周六 0 点起。加密币没有时段，返回 `None`。
pub fn session_started(at: DateTime<Utc>, session: Session) -> Option<DateTime<Utc>> {
    let local = local_of(at);
    let day = local.date_naive();
    let minutes = local.hour() * 60 + local.minute();
    let start = match session {
        Session::Rth => day.and_hms_opt(9, 30, 0)?,
        Session::Off if minutes < 9 * 60 + 30 => {
            // 周一盘前的上一日是周日，周日属于周末，不能当成盘后的起点。
            if local.weekday() == Weekday::Mon {
                day.and_hms_opt(0, 0, 0)?
            } else {
                (day - Duration::days(1)).and_hms_opt(16, 0, 0)?
            }
        }
        Session::Off => day.and_hms_opt(16, 0, 0)?,
        Session::Weekend => {
            let back = i64::from(local.weekday().num_days_from_monday().saturating_sub(5));
            (day - Duration::days(back)).and_hms_opt(0, 0, 0)?
        }
        Session::All => return None,
    };
    Some(utc_of(Utc.from_utc_datetime(&start), at))
}

/// 一个合约在一个时段的断档记录。只给需要身份检查的组用。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BreakMark {
    base: String,
    session: Session,
    /// 连续断档从这一分钟开始。
    since: i64,
    /// 已经记下「这个时段断过」。本身不作废。
    marked: bool,
    /// 在这之前不能拿正常基差当开仓目标。没有禁开时为空。
    #[serde(default)]
    blocked_until: Option<i64>,
}

/// 各组的断档记录，重启后接着记。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct BreakFile {
    groups: HashMap<String, Vec<BreakMark>>,
}

/// 这一分钟的断档结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakStep {
    /// 作废该时段这个时刻之前的样本。
    pub invalidate_before: Option<DateTime<Utc>>,
    /// 重新攒满一个时段之前，不能拿正常基差当开仓目标。
    pub blocked_until: Option<DateTime<Utc>>,
}

fn break_path(dir: &Path) -> std::path::PathBuf {
    dir.join("spread-breaks.json")
}

fn load_breaks(dir: &Path) -> BreakFile {
    std::fs::read_to_string(break_path(dir))
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default()
}

fn save_breaks(dir: &Path, file: &BreakFile) {
    let Ok(text) = serde_json::to_string(file) else {
        return;
    };
    let _ = std::fs::create_dir_all(dir);
    let _ = std::fs::write(break_path(dir), text);
}

/// 需要身份检查的组，记这一分钟断没断。
///
/// 连续满 30 分钟只记账。下一次进入同一时段开头仍断着，才作废断档前的样本。
/// 中间回到同一簇，记录取消。加密币没有下一次时段，连续满 24 小时才作废。
pub fn note_break(
    dir: &Path,
    pair: &str,
    base: &str,
    session: Session,
    minute: i64,
    broken: bool,
) -> BreakStep {
    let mut file = load_breaks(dir);
    let marks = file.groups.entry(pair.to_string()).or_default();
    let slot = marks
        .iter()
        .position(|mark| mark.base.eq_ignore_ascii_case(base) && mark.session == session);
    if !broken {
        if let Some(index) = slot {
            let blocked = marks[index].blocked_until;
            let still = blocked.is_some_and(|until| until > minute);
            if still {
                marks[index].since = minute;
                marks[index].marked = false;
            } else {
                marks.remove(index);
            }
            save_breaks(dir, &file);
            return BreakStep {
                invalidate_before: None,
                blocked_until: still
                    .then(|| DateTime::<Utc>::from_timestamp(blocked.unwrap_or(minute), 0))
                    .flatten(),
            };
        }
        return BreakStep {
            invalidate_before: None,
            blocked_until: None,
        };
    }
    let since = slot.map_or(minute, |index| marks[index].since);
    let elapsed = minute.saturating_sub(since) / 60;
    let mut marked = slot.is_some_and(|index| marks[index].marked) || elapsed >= BREAK_MARK_MINUTES;
    let at = DateTime::<Utc>::from_timestamp(minute, 0).unwrap_or_else(Utc::now);
    let mut step = BreakStep {
        invalidate_before: None,
        blocked_until: None,
    };
    if session == Session::All {
        if elapsed >= CRYPTO_BREAK_HOURS * 60 {
            step.invalidate_before = Some(at);
            step.blocked_until = Some(at + Duration::hours(CRYPTO_BREAK_HOURS));
            marked = false;
        }
    } else if marked && let Some(start) = session_started(at, session) {
        let started = start.timestamp() / 60 * 60;
        if since < started {
            step.invalidate_before = Some(start);
            // 作废后要等这一整段时段重新攒完，才拿新的中位数当目标。
            step.blocked_until = next_session_start(at, session);
            marked = false;
        }
    }
    let next = BreakMark {
        base: base.to_string(),
        session,
        since: step.invalidate_before.map_or(since, |_| minute),
        marked,
        blocked_until: step
            .blocked_until
            .map(|until| until.timestamp())
            .or_else(|| slot.and_then(|index| marks[index].blocked_until)),
    };
    match slot {
        Some(index) => marks[index] = next,
        None => marks.push(next),
    }
    save_breaks(dir, &file);
    step
}

/// 重启时读回还没过期的禁开。运行中不再每秒读文件。
pub fn blocked_now(dir: &Path, pair: &str, now: DateTime<Utc>) -> Vec<(String, Session)> {
    let now = now.timestamp();
    load_breaks(dir)
        .groups
        .get(pair)
        .map(|marks| {
            marks
                .iter()
                .filter(|mark| mark.blocked_until.is_some_and(|until| until > now))
                .map(|mark| (mark.base.clone(), mark.session))
                .collect()
        })
        .unwrap_or_default()
}

/// 下一次同一时段的起点。盘中、盘后跳过周末，周末要到下周六。
fn next_session_start(at: DateTime<Utc>, session: Session) -> Option<DateTime<Utc>> {
    let mut cursor = at + Duration::hours(1);
    for _ in 0..8 * 24 {
        if classify(cursor, false, None) == session
            && session_started(cursor, session).is_some_and(|start| start > at)
        {
            return session_started(cursor, session);
        }
        cursor += Duration::hours(1);
    }
    None
}

/// 某一时刻属于哪个时段。`crypto` 为真时恒为 [`Session::All`]。
/// `outside_rth` 是 Arcus 报的「当前是否在常规时段之外」，`None` = 不知道，按时间推算。
pub fn classify(at: DateTime<Utc>, crypto: bool, outside_rth: Option<bool>) -> Session {
    if crypto {
        return Session::All;
    }
    let local = at + Duration::hours(new_york_offset_hours(at));
    if matches!(local.weekday(), Weekday::Sat | Weekday::Sun) {
        return Session::Weekend;
    }
    match outside_rth {
        Some(true) => Session::Off,
        Some(false) => Session::Rth,
        None => {
            let minutes = local.hour() * 60 + local.minute();
            if (9 * 60 + 30..16 * 60).contains(&minutes) {
                Session::Rth
            } else {
                Session::Off
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn thirty_minutes_only_marks_a_break() {
        let dir = std::env::temp_dir().join(format!("spread-break-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let start = at("2026-07-06T13:30:00Z");
        let first = note_break(&dir, "lighter-rh:hyperliquid-io", "NVDA", Session::Rth, start.timestamp(), true);
        assert!(first.invalidate_before.is_none(), "第一分钟只开始记");
        let marked = note_break(
            &dir,
            "lighter-rh:hyperliquid-io",
            "NVDA",
            Session::Rth,
            (start + Duration::minutes(30)).timestamp(),
            true,
        );
        assert!(marked.invalidate_before.is_none(), "满 30 分钟只记账，不作废");
        assert!(marked.blocked_until.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_break_that_reaches_the_next_session_invalidates_the_old_median() {
        let dir = std::env::temp_dir().join(format!("spread-break-next-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let monday = at("2026-07-06T13:30:00Z");
        note_break(&dir, "p", "NVDA", Session::Rth, monday.timestamp(), true);
        let tuesday = at("2026-07-07T13:30:00Z");
        let step = note_break(&dir, "p", "NVDA", Session::Rth, tuesday.timestamp(), true);
        assert!(step.invalidate_before.is_some(), "下一次盘中开头仍断着才作废");
        assert!(step.blocked_until.is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_break_that_recovers_before_the_next_session_keeps_the_old_samples() {
        let dir = std::env::temp_dir().join(format!("spread-break-back-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let monday = at("2026-07-06T13:30:00Z");
        note_break(&dir, "p", "NVDA", Session::Rth, monday.timestamp(), true);
        note_break(&dir, "p", "NVDA", Session::Rth, (monday + Duration::minutes(30)).timestamp(), true);
        let recovered = note_break(&dir, "p", "NVDA", Session::Rth, (monday + Duration::hours(2)).timestamp(), false);
        assert!(recovered.invalidate_before.is_none());
        let tuesday = at("2026-07-07T13:30:00Z");
        let again = note_break(&dir, "p", "NVDA", Session::Rth, tuesday.timestamp(), true);
        assert!(again.invalidate_before.is_none(), "中间收回来过，第二天开头是新的断档");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
