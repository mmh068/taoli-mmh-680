//! 可开仓机会的旁路记录。
//!
//! 默认不把机会行写入机会日志。手动交易开着时，达标结果写在 `manual-active.jsonl`，
//! 预检、推送和开仓确认读它。后面任一步不达标就删掉；推送成功的最多再留 1 小时。
//! 只有 `ARB_OPPORTUNITY_LOG` 打开时才把同一轮结果追加到文件。不参与扫描、排名、闸门或自动开仓。
//! 文件缺失、建不了目录、写失败都只记一条日志，调用方照常继续。
//! 删掉文件不影响脚本运行。

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

use arb_core::{Decimal, Venue};
use arb_scanner::{Opportunity, ScanReport};
use chrono::{DateTime, SecondsFormat, Utc};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::warn;

use crate::rh_auto::{self, Candidate};
use crate::rh_spread::Line;
use crate::rh_spread::history::{self, Normal};
use crate::rh_spread::session::{self, Session};
use crate::trade::Mode;

/// 旁路正常基差至少要这么多分钟，与价差监控同一口径。
const MIN_MINUTES: usize = history::MIN_MINUTES;
const WINDOW_DAYS: i64 = history::WINDOW_DAYS;

const DEFAULT_PATH: &str = "opportunity-log.jsonl";

/// 手动交易总开关。配置表没写或不是 `on` / `1` / `true` 时关闭。
pub fn enabled() -> bool {
    matches!(
        std::env::var("ARB_MANUAL_TRADE")
            .ok()
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("on" | "1" | "true")
    )
}
/// 手动交易「回到正常净收益」门槛（%）。只读环境变量，不接受页面改写。
/// 没写或写错用 0.05；只接受 0 到 20。
fn configured_net_pct(name: &str, fallback: Decimal) -> Decimal {
    let Ok(raw) = std::env::var(name) else {
        return fallback;
    };
    let Ok(value) = raw.trim().parse::<Decimal>() else {
        warn!("{name} 不是数字，用 {fallback}");
        return fallback;
    };
    if value < Decimal::ZERO || value > Decimal::from(20u32) {
        warn!("{name} 必须在 0 到 20 之间，用 {fallback}");
        return fallback;
    }
    value
}

fn configured_min_net_pct() -> Decimal {
    configured_net_pct("ARB_MANUAL_MIN_NET_PCT", Decimal::new(5, 2))
}

/// 进预检的门槛默认跟写入机会文件相同。没写或写错时用写入门槛，不另起一个数。
fn configured_precheck_min_net_pct() -> Decimal {
    configured_net_pct("ARB_MANUAL_PRECHECK_MIN_NET_PCT", configured_min_net_pct())
}

/// 回到正常净利润 + 资金费 的门槛（%）。没写或写错用 0；只接受 0 到 20。
/// 比较是严格大于，不是大于等于。
fn configured_net_plus_funding_min_pct() -> Decimal {
    configured_net_pct("ARB_NET_PLUS_FUNDING_MIN_PCT", Decimal::ZERO)
}

static MIN_NET_PCT: LazyLock<Decimal> = LazyLock::new(configured_min_net_pct);
static PRECHECK_MIN_NET_PCT: LazyLock<Decimal> = LazyLock::new(configured_precheck_min_net_pct);
static NET_PLUS_FUNDING_MIN_PCT: LazyLock<Decimal> =
    LazyLock::new(configured_net_plus_funding_min_pct);

/// 手动信号已连续出现的扫描轮数。中断或从榜上消失就清零。
static MANUAL_ROUNDS: LazyLock<Mutex<HashMap<String, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
/// 已经从活跃文件读入的手动机会。`None` 表示还没读过磁盘。
static LATEST_MANUAL: LazyLock<Mutex<Option<Vec<Value>>>> = LazyLock::new(|| Mutex::new(None));
const PUSH_KEEP_HOURS: i64 = 1;

/// 连续这么多轮扫描仍达标，才进入预检和推送。不挡住机会文件，也不读自动交易的保持秒数。
const MANUAL_HOLD_ROUNDS: u32 = 2;

static COPIED: LazyLock<Mutex<Option<rh_auto::Settings>>> = LazyLock::new(|| Mutex::new(None));

/// 启动时记一次自动交易设置。之后只在自动设置保存时由 [`remember_auto_settings`] 更新。
pub fn remember_auto_settings(settings: &rh_auto::Settings) {
    if let Ok(mut guard) = COPIED.lock() {
        *guard = Some(settings.clone());
    }
}

pub fn copied_settings() -> rh_auto::Settings {
    COPIED
        .lock()
        .ok()
        .and_then(|guard| guard.clone())
        .unwrap_or_default()
}

/// 手动交易「回到正常净收益」门槛（%）。启动时从 `ARB_MANUAL_MIN_NET_PCT` 读一次。
pub fn min_net_pct() -> Decimal {
    *MIN_NET_PCT
}

/// 进预检的「回到正常净收益」门槛（%）。
/// 启动时从 `ARB_MANUAL_PRECHECK_MIN_NET_PCT` 读一次；没写则与写入门槛相同。
pub fn precheck_min_net_pct() -> Decimal {
    *PRECHECK_MIN_NET_PCT
}

/// 回到正常净利润 + 资金费 必须大于的门槛（%）。启动时从 `ARB_NET_PLUS_FUNDING_MIN_PCT` 读一次，默认 0。
pub fn net_plus_funding_min_pct() -> Decimal {
    *NET_PLUS_FUNDING_MIN_PCT
}

/// 净利润已经过了自己的门槛之后，再加上资金费。收为正、付为负。必须严格大于配置值。
pub fn net_plus_funding_clears(net: Decimal, funding: Decimal) -> bool {
    net + funding > net_plus_funding_min_pct()
}

/// 这一行是否达到进预检的回到正常净利润门槛。缺数字不算达标。
/// 先比不含资金费的回到正常净利润，再要求这笔净利润加上资金费严格大于配置值。
/// 同时还要符合自动交易的可成交价差标准：价差为正，扣掉平仓穿价和往返手续费后仍为正，
/// 且回到正常的净利润不低于自动交易面板上的门槛。资金费算不出来不猜。
pub fn meets_precheck(row: &Value) -> bool {
    if !meets_executable_spread(row) {
        return false;
    }
    let Some(net) = return_net(row) else {
        return false;
    };
    if net < precheck_floor() {
        return false;
    }
    let Some(funding) = row_pct(row, "funding_pct") else {
        return false;
    };
    net_plus_funding_clears(net, funding)
}

/// 自动交易下单前的硬性价差条件：可成交价差为正，收敛到 0 扣完成本仍为正。
pub fn meets_executable_spread(row: &Value) -> bool {
    let Some(entry) = row_pct(row, "entry_pct") else {
        return false;
    };
    let Some(net) = row_pct(row, "price_net_pct") else {
        return false;
    };
    entry > Decimal::ZERO && net > Decimal::ZERO
}

pub fn precheck_floor_pct() -> Decimal {
    precheck_min_net_pct().max(copied_settings().min_net_pct)
}

fn precheck_floor() -> Decimal {
    precheck_floor_pct()
}

fn row_pct(row: &Value, field: &str) -> Option<Decimal> {
    row[field]
        .as_str()
        .and_then(|text| text.parse::<Decimal>().ok())
}

/// 回到正常净利润。用价差净收益减正常基差现算，不直接信 `net_to_normal_pct`。
/// 旧行把 8 小时资金费加进了这个字段；再拿去加资金费就会算两次。
/// 和现算结果差在 0.0002 以内时，沿用写入时的四位小数，避免边界来回跳。
fn return_net(row: &Value) -> Option<Decimal> {
    let price = row_pct(row, "price_net_pct")?;
    let normal = row_pct(row, "normal_basis_pct")?;
    let stripped = (price - normal).round_dp(4);
    match row_pct(row, "net_to_normal_pct") {
        Some(stored) if (stored - stripped).abs() <= Decimal::new(2, 4) => Some(stored),
        _ => Some(stripped),
    }
}

/// 推送文案。回到正常净利润不含资金费。金额按净利润加 8 小时资金费折算。
pub fn manual_push_text(
    base: &str,
    long: &str,
    short: &str,
    size: Decimal,
    leverage: Decimal,
    entry_pct: Decimal,
    normal_basis_pct: Decimal,
    basis_pct: Decimal,
    session: &str,
    net_pct: Decimal,
    funding_pct: Decimal,
) -> String {
    let sum = net_pct + funding_pct;
    let profit_usdt = (sum / Decimal::ONE_HUNDRED * size)
        .round_dp(2)
        .normalize();
    let long_label = venue_name(long);
    let short_label = venue_name(short);
    format!(
        "手动机会已通过预检：{base} 多 {long_label} / 空 {short_label}，{} USDT × {}x\n可成交价差 {}%（{} USDT），{session}正常基差 {}%，当前 {}%（{short_label} − {long_label}）\n回到正常净利润 {}%（不含资金费，已扣手续费与平仓穿价）\n回到正常净利润 + 8 小时资金费 {}%（资金费 {}%，≈{profit_usdt} USDT）\n30 分钟内有效，用过即废。下单前仍重查，资金费固定按 8 小时，不看自动面板。",
        size.normalize(),
        leverage.normalize(),
        entry_pct.round_dp(3).normalize(),
        size.normalize(),
        normal_basis_pct.round_dp(3).normalize(),
        basis_pct.round_dp(3).normalize(),
        net_pct.round_dp(3).normalize(),
        sum.round_dp(3).normalize(),
        funding_pct.round_dp(3).normalize(),
        profit_usdt = profit_usdt,
    )
}

/// 通过预检：回到正常净利润不含资金费，先和进预检的门槛比。过了再要求加上 8 小时资金费后严格大于配置值。不吃深度，也不读自动面板的小时数。
pub fn manual_pass_block(
    report: &ScanReport,
    base: &str,
    long: Venue,
    short: Venue,
    floor_pct: Decimal,
) -> Option<String> {
    let Some(normal) = paired_normal_basis_pct(base, long, short) else {
        return Some("正常基差算不出来，这次不通过预检".into());
    };
    let Some(opportunity) = find_spread(report, base, long, short) else {
        return Some("这轮扫描没有这对，这次不通过预检".into());
    };
    let Some(entry) = opportunity.executable_basis_pct else {
        return Some("可成交价差算不出来，这次不通过预检".into());
    };
    if entry <= Decimal::ZERO {
        return Some("可成交价差不为正：空腿卖出价要高于多腿买入价".into());
    }
    let price_net = (opportunity.spread_net * Decimal::ONE_HUNDRED).round_dp(4);
    if price_net <= Decimal::ZERO {
        return Some(format!(
            "可成交价差 {}% 扣掉平仓穿价与往返手续费后不划算（净 {}%）",
            entry.round_dp(3).normalize(),
            price_net.round_dp(3).normalize()
        ));
    }
    let net = quote_net_to_normal(price_net, normal);
    if net < floor_pct {
        return Some(format!(
            "回到正常净利润 {}% 低于门槛 {}%（不含资金费：一档可成交价差已扣平仓穿价与往返手续费，再减去正常基差 {}%）",
            net.round_dp(3).normalize(),
            floor_pct.normalize(),
            normal.round_dp(3).normalize()
        ));
    }
    let Some(funding) = funding_8h_pct(opportunity) else {
        return Some("8 小时资金费算不出来，这次不通过预检".into());
    };
    if !net_plus_funding_clears(net, funding) {
        let sum = (net + funding).round_dp(4);
        return Some(format!(
            "回到正常净利润 {}% + 8 小时资金费 {}% = {}%，不高于门槛 {}%，这次不通过预检",
            net.round_dp(3).normalize(),
            funding.round_dp(3).normalize(),
            sum.normalize(),
            net_plus_funding_min_pct().normalize()
        ));
    }
    None
}

fn venue_name(name: &str) -> String {
    Venue::parse(name)
        .map(crate::rh_spread::venue_label)
        .unwrap_or(name)
        .to_string()
}

/// 最近一轮里、已经达到预检标准的这组手动机会的价差。
pub struct ManualSpread {
    pub entry_pct: Decimal,
    pub normal_basis_pct: Decimal,
    pub basis_pct: Decimal,
    pub session_label: String,
    pub net_to_normal_pct: Decimal,
    pub funding_pct: Decimal,
}

pub fn spread_for_push(base: &str, long: Venue, short: Venue) -> Option<ManualSpread> {
    qualifying_manual().into_iter().find_map(|row| {
        if row["base"] != base || row["long"] != long.as_str() || row["short"] != short.as_str() {
            return None;
        }
        if row["held_rounds"].as_u64().unwrap_or(0) < u64::from(MANUAL_HOLD_ROUNDS)
            || !meets_precheck(&row)
        {
            return None;
        }
        Some(ManualSpread {
            entry_pct: row_pct(&row, "entry_pct")?,
            normal_basis_pct: row_pct(&row, "normal_basis_pct")?,
            basis_pct: row_pct(&row, "basis_pct")?,
            session_label: session_from_name(row["normal_session"].as_str().unwrap_or(""))
                .map(Session::label)
                .unwrap_or("当前")
                .to_string(),
            net_to_normal_pct: return_net(&row)?,
            funding_pct: row_pct(&row, "funding_pct")?,
        })
    })
}

/// 正常基差样本文件。跟机会记录放在同一目录，文件名加 `-basis`。
/// 删掉它只是重新攒样本，不影响开仓。
/// 只记已经达到机会文件写入标准的配对。机会文件关掉时也记，手动流程还要靠它算正常基差。
/// 文件只留最近 [`WINDOW_DAYS`] 天；更早的行在下一次写入时丢掉。
pub fn basis_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("opportunity-log.jsonl");
    let basis = match name.rsplit_once('.') {
        Some((stem, ext)) => format!("{stem}-basis.{ext}"),
        None => format!("{name}-basis"),
    };
    // Windows 短文件名和长文件名指向同一目录。不先规范化，基差样本和活跃机会会写到两条路径上。
    let parent = path.parent().filter(|parent| !parent.as_os_str().is_empty());
    let parent = parent
        .and_then(|parent| std::fs::canonicalize(parent).ok())
        .unwrap_or_else(|| parent.unwrap_or(Path::new("")).to_path_buf());
    parent.join(basis)
}

/// 这一轮用的基差文件。机会文件有路径时跟在旁边；关掉时用默认文件名。
pub fn basis_store_path() -> PathBuf {
    basis_path(
        path_from_env()
            .as_deref()
            .unwrap_or(Path::new(DEFAULT_PATH)),
    )
}

/// 最近一轮里某个手动机会的回到正常净收益（%）。没有或过期则为空。
/// 最近一轮达标手动机会记下的正常基差（%，空腿减多腿）。
pub fn paired_normal_basis_pct(base: &str, long: Venue, short: Venue) -> Option<Decimal> {
    qualifying_manual().into_iter().find_map(|row| {
        if !same_pair(&row, base, long, short) {
            return None;
        }
        row_pct(&row, "normal_basis_pct")
    })
}

pub fn manual_net_to_normal(base: &str, long: Venue, short: Venue) -> Option<String> {
    qualifying_manual().into_iter().find_map(|row| {
        (row["base"] == base && row["long"] == long.as_str() && row["short"] == short.as_str())
            .then(|| row["net_to_normal_pct"].as_str().map(str::to_string))
            .flatten()
    })
}

/// 最近一轮里，已经算出回到正常净收益且达到页面门槛的手动机会。
pub fn qualifying_manual() -> Vec<Value> {
    if !enabled() {
        return Vec::new();
    }
    qualifying_from_latest()
}

fn qualifying_from_latest() -> Vec<Value> {
    if let Ok(guard) = LATEST_MANUAL.lock()
        && let Some(rows) = guard.as_ref()
    {
        return rows.clone();
    }
    load_active(&active_store_path())
}

pub fn path_from_env() -> Option<PathBuf> {
    path_from_setting(std::env::var("ARB_OPPORTUNITY_LOG").ok().as_deref())
}

/// 机会文件开关。不写、空、`off`、`0`、`false` 都不记。
/// `on`、`1`、`true` 写到当前目录的 `opportunity-log.jsonl`；其它值当作路径。
fn path_from_setting(raw: Option<&str>) -> Option<PathBuf> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return None;
    };
    match raw.to_ascii_lowercase().as_str() {
        "off" | "0" | "false" => None,
        "on" | "1" | "true" => Some(PathBuf::from(DEFAULT_PATH)),
        _ => Some(PathBuf::from(raw)),
    }
}

/// 扫描榜上可成交价差为正、扣费后仍为正的配对。手动开仓看的就是这一档。
pub fn record_scan(path: Option<&Path>, report: &ScanReport) {
    if !enabled() {
        return;
    }
    record_enabled_scan(path, report);
}

fn record_enabled_scan(path: Option<&Path>, report: &ScanReport) {
    let basis = basis_path(path.unwrap_or(Path::new(DEFAULT_PATH)));
    record_at(path, &basis, report);
}

fn record_at(path: Option<&Path>, basis: &Path, report: &ScanReport) {
    let at = report.generated_at;
    let normals = note_basis(basis, at);
    let mut rows = Vec::new();
    let mut still = HashSet::new();
    let mut entered = Vec::new();
    for (_view, opportunity) in top_spread(report, normals.as_ref(), MANUAL_TOP) {
        let key = format!(
            "{}|{}|{}",
            opportunity.symbol.base,
            opportunity.long.as_str(),
            opportunity.short.as_str()
        );
        let Some(mut row) = manual_row(at, opportunity, normals.as_ref()) else {
            continue;
        };
        // 价差条件还在就续计轮次。没满两轮也写入，只是不进预检。
        still.insert(key.clone());
        entered.push(opportunity);
        let rounds = manual_rounds(&key);
        row["held_rounds"] = json!(rounds);
        row["hold_rounds"] = json!(MANUAL_HOLD_ROUNDS);
        rows.push(row);
    }
    if let Ok(mut seen) = MANUAL_ROUNDS.lock() {
        seen.retain(|key, _| still.contains(key));
    }
    publish_manual(basis, &rows, at);
    let keep_basis = match path {
        Some(path) => append(path, &rows),
        None => true,
    };
    if keep_basis {
        remember_entered(basis, at, &entered);
    }
}

fn publish_manual(basis: &Path, rows: &[Value], now: DateTime<Utc>) {
    let path = active_path_for(basis);
    let merged = merge_active(&load_active(&path), rows, now);
    store_active(&path, &merged);
    if let Ok(mut latest) = LATEST_MANUAL.lock() {
        *latest = Some(merged);
    }
}

fn active_path_for(basis: &Path) -> PathBuf {
    basis.with_file_name("manual-active.jsonl")
}

fn active_store_path() -> PathBuf {
    active_path_for(&basis_store_path())
}

/// 推送已经发出。从这一刻起最多再留 1 小时，到点就从活跃文件去掉。
pub fn mark_pushed(base: &str, long: Venue, short: Venue) {
    revise_active(|rows| {
        let Some(row) = rows
            .iter_mut()
            .find(|row| same_pair(row, base, long, short))
        else {
            return;
        };
        if row["pushed_at"].as_str().is_none() {
            row["pushed_at"] = json!(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true));
        }
    });
}

/// 后续步骤已经不达标。马上从活跃文件去掉，不管是不是刚推送过。
pub fn drop_active(base: &str, long: Venue, short: Venue) {
    revise_active(|rows| {
        rows.retain(|row| !same_pair(row, base, long, short));
    });
}

fn revise_active(edit: impl FnOnce(&mut Vec<Value>)) {
    let path = active_store_path();
    let mut rows = load_active(&path);
    edit(&mut rows);
    store_active(&path, &rows);
    if let Ok(mut latest) = LATEST_MANUAL.lock() {
        *latest = Some(rows);
    }
}

fn same_pair(row: &Value, base: &str, long: Venue, short: Venue) -> bool {
    row["base"]
        .as_str()
        .unwrap_or_default()
        .eq_ignore_ascii_case(base)
        && row["long"].as_str() == Some(long.as_str())
        && row["short"].as_str() == Some(short.as_str())
}

/// 这一轮还达标的留下；不达标的删掉。推送成功未满 1 小时的先留着，供开仓确认。
fn merge_active(existing: &[Value], incoming: &[Value], now: DateTime<Utc>) -> Vec<Value> {
    let mut prior: HashMap<String, Value> = existing
        .iter()
        .map(|row| (active_key(row), row.clone()))
        .collect();
    let mut out = HashMap::new();
    for row in incoming.iter().filter(|row| flow_row(row)) {
        let key = active_key(row);
        let mut row = row.clone();
        if let Some(old) = prior.remove(&key)
            && pushed_recent(&old, now)
        {
            row["pushed_at"] = old["pushed_at"].clone();
        }
        out.insert(key, row);
    }
    for (key, old) in prior {
        if pushed_recent(&old, now) {
            out.insert(key, old);
        }
    }
    let mut rows: Vec<Value> = out.into_values().collect();
    rows.sort_by(|left, right| active_key(left).cmp(&active_key(right)));
    rows
}

fn flow_row(row: &Value) -> bool {
    row["kind"] == "manual"
        && row["openable"] == true
        && row["held_rounds"].as_u64().unwrap_or(0) >= u64::from(MANUAL_HOLD_ROUNDS)
        && meets_precheck(row)
}

fn pushed_recent(row: &Value, now: DateTime<Utc>) -> bool {
    row["pushed_at"]
        .as_str()
        .and_then(|text| text.parse::<DateTime<Utc>>().ok())
        .is_some_and(|at| now.signed_duration_since(at) <= chrono::Duration::hours(PUSH_KEEP_HOURS))
}

fn active_key(row: &Value) -> String {
    format!(
        "{}|{}|{}",
        row["base"].as_str().unwrap_or_default(),
        row["long"].as_str().unwrap_or_default(),
        row["short"].as_str().unwrap_or_default()
    )
}

fn load_active(path: &Path) -> Vec<Value> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    text.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect()
}

fn store_active(path: &Path, rows: &[Value]) {
    if let Some(dir) = path.parent()
        && !dir.as_os_str().is_empty()
        && let Err(error) = std::fs::create_dir_all(dir)
    {
        warn!(path = %path.display(), "手动机会活跃文件目录建不了：{error}");
        return;
    }
    let mut text = String::new();
    for row in rows {
        let Ok(line) = serde_json::to_string(row) else {
            continue;
        };
        text.push_str(&line);
        text.push('\n');
    }
    let tmp = path.with_extension("tmp");
    if let Err(error) = std::fs::write(&tmp, text) {
        warn!(path = %path.display(), "手动机会活跃文件没写上：{error}");
        return;
    }
    if let Err(error) = std::fs::rename(&tmp, path) {
        warn!(path = %path.display(), "手动机会活跃文件没换上：{error}");
    }
}

fn manual_rounds(key: &str) -> u32 {
    let Ok(mut seen) = MANUAL_ROUNDS.lock() else {
        return 0;
    };
    let rounds = seen.entry(key.to_string()).or_insert(0);
    *rounds = rounds.saturating_add(1);
    *rounds
}

const MANUAL_TOP: usize = 18;

/// 每个币只留机会净收益最大的那一对，再取全市场前 N 条。
/// 有正常基差时比回到正常的净收益（一档价差扣费后再减正常基差，加 8 小时资金费）；没有时比价差净收益加 8 小时资金费。
fn top_spread<'a>(
    report: &'a ScanReport,
    normals: Option<&HashMap<String, Normal>>,
    limit: usize,
) -> Vec<(&'a arb_scanner::SymbolView, &'a Opportunity)> {
    let mut ranked: Vec<_> = report
        .symbols
        .iter()
        .filter_map(|view| {
            let opportunity = view.spread.iter().max_by(|left, right| {
                selection_net(left, normals)
                    .cmp(&selection_net(right, normals))
                    .then_with(|| right.long.cmp(&left.long))
                    .then_with(|| right.short.cmp(&left.short))
            })?;
            Some((view, opportunity))
        })
        .collect();
    ranked.sort_by(|(left_view, left), (right_view, right)| {
        selection_net(right, normals)
            .cmp(&selection_net(left, normals))
            .then_with(|| left_view.symbol.base.cmp(&right_view.symbol.base))
            .then_with(|| left.long.cmp(&right.long))
            .then_with(|| left.short.cmp(&right.short))
    });
    ranked.truncate(limit);
    ranked
}

fn selection_net(opportunity: &Opportunity, normals: Option<&HashMap<String, Normal>>) -> Decimal {
    let price = (opportunity.spread_net * Decimal::ONE_HUNDRED).round_dp(4);
    let Some(normals) = normals else {
        return price;
    };
    let key = basis_key(
        &opportunity.symbol.base,
        opportunity.long,
        opportunity.short,
        session_of(),
    );
    let Some(normal) = normals.get(&key) else {
        return price;
    };
    let Some(normal_pct) = Decimal::from_f64_retain(normal.median) else {
        return price;
    };
    quote_net_to_normal(price, normal_pct)
}

/// 一档回到正常净利润（%）：可成交价差扣掉平仓穿价和往返手续费，再减去正常基差。不含资金费。
fn quote_net_to_normal(price_net_pct: Decimal, normal_pct: Decimal) -> Decimal {
    (price_net_pct - normal_pct).round_dp(4)
}

/// 未来 8 小时资金费（%）。空腿收、多腿付，按扫描的日化费差摊到 8 小时。结算周期无效时不算。
fn funding_8h_pct(opportunity: &Opportunity) -> Option<Decimal> {
    if opportunity.long_interval_h == 0 || opportunity.short_interval_h == 0 {
        return None;
    }
    let pct = opportunity.daily_spread * Decimal::from(8u32) / Decimal::from(24u32)
        * Decimal::ONE_HUNDRED;
    Some(pct.round_dp(4))
}

fn find_spread<'a>(
    report: &'a ScanReport,
    base: &str,
    long: Venue,
    short: Venue,
) -> Option<&'a Opportunity> {
    report.symbols.iter().find_map(|view| {
        view.symbol
            .base
            .eq_ignore_ascii_case(base)
            .then(|| {
                view.spread
                    .iter()
                    .find(|opportunity| opportunity.long == long && opportunity.short == short)
            })
            .flatten()
    })
}

/// 监控自己已经标成信号、但自动交易还没按面板门槛计时的一行。
pub fn record_monitor_signal(path: Option<&Path>, at: DateTime<Utc>, line: &Line) {
    let Some(path) = path else {
        return;
    };
    let Some(best) = line.best.as_ref().filter(|best| best.signal) else {
        return;
    };
    let Some((long, short, leg)) = line.legs(best.direction) else {
        return;
    };
    let Some(net) = leg.net_to_normal_pct else {
        return;
    };
    if leg.quote.entry_pct <= Decimal::ZERO || leg.net_to_zero_pct <= Decimal::ZERO {
        return;
    }
    let row = row(
        at,
        "auto",
        &line.base,
        "USDT",
        long,
        short,
        leg.quote.entry_pct,
        net,
        Some(best.signal_sec),
        None,
        None,
        best.signal_sec >= crate::rh_spread::SIGNAL_HOLD.as_secs(),
        (best.signal_sec < crate::rh_spread::SIGNAL_HOLD.as_secs())
            .then_some("监控信号保持时间还没到提醒门槛"),
    );
    if remember_auto(&row) {
        append(path, &[row]);
    }
}

/// 价差监控里已经达到自动开仓条件的一行。没保持够、在排除名单里的不记。
/// 记下来只说明当时符合条件，不表示已经下单。
///
/// 同一条信号（合约、两腿、门槛）只在新出现时记一条，避免每秒重复。
/// 信号消失后再出现，会再记一条。
pub fn record_auto(
    path: Option<&Path>,
    at: DateTime<Utc>,
    line: &Line,
    settings: &rh_auto::Settings,
    report: Option<&ScanReport>,
    held_sec: u64,
    mode: Option<Mode>,
) {
    let Some(path) = path else {
        return;
    };
    let Some(row) = auto_row(at, line, settings, report, held_sec, mode) else {
        return;
    };
    if row["openable"] != true {
        return;
    }
    if !remember_auto(&row) {
        return;
    }
    append(path, &[row]);
}

fn remember_auto(row: &Value) -> bool {
    static SEEN: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));
    let key = format!(
        "{}|{}|{}|{}",
        row["base"].as_str().unwrap_or_default(),
        row["long"].as_str().unwrap_or_default(),
        row["short"].as_str().unwrap_or_default(),
        row["min_net_pct"].as_str().unwrap_or_default()
    );
    let Ok(mut seen) = SEEN.lock() else {
        return true;
    };
    if seen.contains(&key) {
        return false;
    }
    // 只用于去重，不参与开仓。键太多时清掉，下一轮最多多记一次。
    if seen.len() >= 4_000 {
        seen.clear();
    }
    seen.insert(key);
    true
}

fn manual_row(
    at: DateTime<Utc>,
    opportunity: &Opportunity,
    normals: Option<&HashMap<String, Normal>>,
) -> Option<Value> {
    if !opportunity.spread_profitable() || opportunity.oi_capped {
        return None;
    }
    let entry_pct = opportunity.executable_basis_pct?;
    if entry_pct <= Decimal::ZERO {
        return None;
    }
    let key = basis_key(
        &opportunity.symbol.base,
        opportunity.long,
        opportunity.short,
        session_of(),
    );
    let normal = normals.and_then(|map| map.get(&key));
    let price_net_pct = (opportunity.spread_net * Decimal::ONE_HUNDRED).round_dp(4);
    let funding_pct = funding_8h_pct(opportunity);
    let net_pct = price_net_pct;
    let mut value = row(
        at,
        "manual",
        &opportunity.symbol.base,
        &opportunity.symbol.quote,
        opportunity.long,
        opportunity.short,
        entry_pct,
        net_pct,
        None,
        None,
        None,
        !opportunity.oi_capped,
        opportunity
            .oi_capped
            .then_some("持仓量已到上限，现在开不出来"),
    );
    value["price_net_pct"] = json!(price_net_pct.normalize().to_string());
    value["funding_pct"] = funding_pct.map_or(Value::Null, |funding| {
        json!(funding.normalize().to_string())
    });
    value["hold_days"] = json!(opportunity.spread_hold_days.normalize().to_string());
    if funding_pct.is_none() {
        value["funding_note"] = json!("结算周期无效，8 小时资金费算不出来，这次不算合格");
    }
    attach_normal(&mut value, normal, opportunity.entry_basis_pct);
    let min_net = min_net_pct();
    let precheck_min = precheck_min_net_pct();
    let net = value["net_to_normal_pct"]
        .as_str()
        .and_then(|text| text.parse::<Decimal>().ok());
    let funding = row_pct(&value, "funding_pct");
    let meets = net.is_some_and(|net| {
        net >= min_net && funding.is_some_and(|funding| net_plus_funding_clears(net, funding))
    });
    value["min_net_pct"] = json!(min_net.normalize().to_string());
    value["precheck_min_net_pct"] = json!(precheck_min.normalize().to_string());
    value["net_plus_funding_min_pct"] =
        json!(net_plus_funding_min_pct().normalize().to_string());
    if let (Some(net), Some(funding)) = (net, funding) {
        value["net_plus_funding_pct"] =
            json!((net + funding).round_dp(4).normalize().to_string());
    }
    if let Some(net) = net {
        let size = copied_settings().size_usdt;
        value["size_usdt"] = json!(size.normalize().to_string());
        value["net_to_normal_usdt"] = json!(
            (net / Decimal::ONE_HUNDRED * size)
                .round_dp(4)
                .normalize()
                .to_string()
        );
    }
    value["openable"] = json!(meets && !opportunity.oi_capped);
    if net.is_none() {
        value["block"] = json!("正常基差样本不够，算不出回到正常的净收益");
        return Some(value);
    }
    // 已经能算、但低于页面门槛：不收进机会文件。
    if !meets {
        return None;
    }
    if opportunity.oi_capped {
        value["block"] = json!("持仓量已到上限，现在开不出来");
    }
    Some(value)
}

/// 只用已经落盘的样本算正常基差。这一轮还没决定收谁，不能先写进去。
fn note_basis(path: &Path, now: DateTime<Utc>) -> Option<HashMap<String, Normal>> {
    match load_basis(path) {
        Ok(samples) => Some(normals_of(&samples, now)),
        Err(error) => {
            warn!(path = %path.display(), "正常基差算不出来，机会行里留空：{error}");
            None
        }
    }
}

/// 给已经达到写入标准的配对记这一分钟的标记价基差。`path` 是基差文件本身。
fn remember_entered(path: &Path, at: DateTime<Utc>, entered: &[&Opportunity]) {
    let minute = at.timestamp() / 60 * 60;
    let session = session_of();
    let mut fresh = Vec::new();
    for opportunity in entered {
        let Some(basis) = opportunity.entry_basis_pct.and_then(|basis| basis.to_f64()) else {
            continue;
        };
        if !basis.is_finite() {
            continue;
        }
        fresh.push(BasisSample {
            t: minute,
            base: opportunity.symbol.base.clone(),
            long: opportunity.long.as_str().to_string(),
            short: opportunity.short.as_str().to_string(),
            session,
            basis,
        });
    }
    if fresh.is_empty() {
        return;
    }
    let file = path;
    let mut seen = match recent_minutes(&file, minute) {
        Ok(seen) => seen,
        Err(error) => {
            warn!(path = %file.display(), "正常基差样本读不了，这一轮照样追加：{error}");
            HashSet::new()
        }
    };
    fresh.retain(|sample| {
        seen.insert((
            sample.t,
            sample.base.clone(),
            sample.long.clone(),
            sample.short.clone(),
            sample.session,
        ))
    });
    if !fresh.is_empty() {
        append_basis(&file, &fresh);
    }
}

fn recent_minutes(
    path: &Path,
    minute: i64,
) -> std::io::Result<HashSet<(i64, String, String, String, Session)>> {
    let samples = load_basis(path)?;
    Ok(samples
        .into_iter()
        .filter(|sample| sample.t == minute)
        .map(|sample| {
            (
                sample.t,
                sample.base,
                sample.long,
                sample.short,
                sample.session,
            )
        })
        .collect())
}

fn session_of() -> Session {
    // 扫描快照没有「是不是加密币」这一项。按纽约时段分，不把股票误标成全天。
    session::classify(Utc::now(), false, None)
}

fn session_key(session: Session) -> &'static str {
    match session {
        Session::Rth => "rth",
        Session::Off => "off",
        Session::Weekend => "weekend",
        Session::All => "all",
    }
}

fn session_from_name(name: &str) -> Option<Session> {
    match name {
        "rth" => Some(Session::Rth),
        "off" => Some(Session::Off),
        "weekend" => Some(Session::Weekend),
        "all" => Some(Session::All),
        _ => None,
    }
}

fn basis_key(base: &str, long: Venue, short: Venue, session: Session) -> String {
    format!(
        "{base}|{}|{}|{}",
        long.as_str(),
        short.as_str(),
        session_key(session)
    )
}

/// 开仓用的回到正常净利润（%）。不含资金费。资金费在下单时按当时快照另加。
pub fn manual_basis_net(base: &str, long: Venue, short: Venue) -> Option<Decimal> {
    qualifying_manual().into_iter().find_map(|row| {
        if !same_pair(&row, base, long, short) {
            return None;
        }
        return_net(&row)
    })
}

/// 用信号记下的那个时段重算平仓目标。基差样本已是「空 − 多」，目标就是该时段的中位数。
/// 样本不够，或目标超出规则允许的范围时，这次不开仓。
pub fn manual_close_target(base: &str, long: Venue, short: Venue) -> Result<Decimal, String> {
    let session_name = qualifying_manual()
        .into_iter()
        .find_map(|row| {
            same_pair(&row, base, long, short)
                .then(|| row["normal_session"].as_str().map(str::to_string))
                .flatten()
        })
        .ok_or("这组机会没有记下正常基差的时段，这次不开仓")?;
    let session = session_from_name(&session_name).ok_or("正常基差的时段认不出，这次不开仓")?;
    let samples =
        load_basis(&basis_store_path()).map_err(|error| format!("正常基差读不了：{error}"))?;
    let values = samples
        .into_iter()
        .filter(|sample| {
            sample.base.eq_ignore_ascii_case(base)
                && sample.long == long.as_str()
                && sample.short == short.as_str()
                && sample.session == session
                && sample.basis.is_finite()
        })
        .map(|sample| sample.basis)
        .collect();
    position_target(values)
}

fn position_target(mut values: Vec<f64>) -> Result<Decimal, String> {
    if values.len() < MIN_MINUTES {
        return Err(format!(
            "同一时段的正常基差还不到 {MIN_MINUTES} 分钟，这次不开仓"
        ));
    }
    values.sort_by(f64::total_cmp);
    let median = history::percentile(&values, 0.5);
    let Some(value) = Decimal::from_f64_retain(median) else {
        return Err("正常基差算不出来，这次不开仓".into());
    };
    let value = value.round_dp(3);
    if value.abs() > arb_exec::monitor::MAX_BASIS_EXIT_PCT {
        return Err(format!(
            "收敛目标 {}% 超出 ±{}%，这次不开仓",
            value.normalize(),
            arb_exec::monitor::MAX_BASIS_EXIT_PCT
        ));
    }
    Ok(value)
}

fn normals_of(samples: &[BasisSample], now: DateTime<Utc>) -> HashMap<String, Normal> {
    let cutoff = (now - chrono::Duration::days(WINDOW_DAYS)).timestamp();
    let mut grouped: HashMap<(String, String, String, Session), Vec<f64>> = HashMap::new();
    for sample in samples {
        if sample.t < cutoff || !sample.basis.is_finite() {
            continue;
        }
        grouped
            .entry((
                sample.base.clone(),
                sample.long.clone(),
                sample.short.clone(),
                sample.session,
            ))
            .or_default()
            .push(sample.basis);
    }
    let mut out = HashMap::new();
    for ((base, long, short, session), mut values) in grouped {
        if values.len() < MIN_MINUTES {
            continue;
        }
        values.sort_by(f64::total_cmp);
        let mut deviations: Vec<f64> = values
            .iter()
            .map(|value| (value - history::percentile(&values, 0.5)).abs())
            .collect();
        deviations.sort_by(f64::total_cmp);
        out.insert(
            format!("{base}|{long}|{short}|{}", session_key(session)),
            Normal {
                session,
                median: history::percentile(&values, 0.5),
                p10: history::percentile(&values, 0.1),
                p90: history::percentile(&values, 0.9),
                mad: history::percentile(&deviations, 0.5),
                minutes: values.len(),
            },
        );
    }
    out
}

fn attach_normal(value: &mut Value, normal: Option<&Normal>, mark_basis: Option<Decimal>) {
    let Some(normal) = normal else {
        value["normal_basis_pct"] = Value::Null;
        value["normal_minutes"] = Value::Null;
        value["net_to_normal_pct"] = Value::Null;
        return;
    };
    value["normal_basis_pct"] = json!(format!("{:.4}", normal.median));
    value["normal_session"] = json!(normal.session);
    value["basis_pct"] = mark_basis.map_or(Value::Null, |basis| {
        json!(basis.round_dp(4).normalize().to_string())
    });
    value["normal_minutes"] = json!(normal.minutes);
    let price_net = value["price_net_pct"]
        .as_str()
        .and_then(|text| text.parse::<Decimal>().ok());
    let normal_pct = Decimal::from_f64_retain(normal.median);
    value["net_to_normal_pct"] = match (price_net, normal_pct) {
        (Some(price), Some(normal_pct)) => {
            json!(format!("{:.4}", quote_net_to_normal(price, normal_pct)))
        }
        _ => Value::Null,
    };
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BasisSample {
    t: i64,
    base: String,
    long: String,
    short: String,
    session: Session,
    basis: f64,
}

fn load_basis(path: &Path) -> std::io::Result<Vec<BasisSample>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let cutoff = (Utc::now() - chrono::Duration::days(WINDOW_DAYS)).timestamp();
    let mut out = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(sample) = serde_json::from_str::<BasisSample>(line) else {
            continue;
        };
        if sample.t >= cutoff {
            out.push(sample);
        }
    }
    Ok(out)
}

fn append_basis(path: &Path, rows: &[BasisSample]) {
    let _guard = basis_write()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if basis_needs_rewrite(path) {
        let mut kept = match load_basis(path) {
            Ok(samples) => samples,
            Err(error) => {
                warn!(path = %path.display(), "基差样本收不起来，这一批仍追加：{error}");
                let values: Vec<Value> = rows
                    .iter()
                    .filter_map(|row| serde_json::to_value(row).ok())
                    .collect();
                append(path, &values);
                return;
            }
        };
        kept.extend(rows.iter().cloned());
        if let Err(error) = rewrite_basis(path, &kept) {
            warn!(path = %path.display(), "基差样本没有收成最近 {WINDOW_DAYS} 天：{error}");
            let values: Vec<Value> = rows
                .iter()
                .filter_map(|row| serde_json::to_value(row).ok())
                .collect();
            append(path, &values);
        }
        return;
    }
    let values: Vec<Value> = rows
        .iter()
        .filter_map(|row| serde_json::to_value(row).ok())
        .collect();
    append(path, &values);
}

fn basis_write() -> &'static Mutex<()> {
    static LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
    &LOCK
}

/// 最早一行已经超出回看窗口时，整份重写，只留窗口内的样本。
fn basis_needs_rewrite(path: &Path) -> bool {
    let cutoff = (Utc::now() - chrono::Duration::days(WINDOW_DAYS)).timestamp();
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return false,
    };
    let mut reader = std::io::BufReader::new(file);
    let mut line = String::new();
    loop {
        line.clear();
        let read = match std::io::BufRead::read_line(&mut reader, &mut line) {
            Ok(read) => read,
            Err(_) => return false,
        };
        if read == 0 {
            return false;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        return match serde_json::from_str::<BasisSample>(trimmed) {
            Ok(sample) => sample.t < cutoff,
            Err(_) => true,
        };
    }
}

fn rewrite_basis(path: &Path, rows: &[BasisSample]) -> std::io::Result<()> {
    let cutoff = (Utc::now() - chrono::Duration::days(WINDOW_DAYS)).timestamp();
    let values: Vec<Value> = rows
        .iter()
        .filter(|row| row.t >= cutoff && row.basis.is_finite())
        .filter_map(|row| serde_json::to_value(row).ok())
        .collect();
    if let Some(dir) = path.parent()
        && !dir.as_os_str().is_empty()
    {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = {
        let mut name = path.as_os_str().to_owned();
        name.push(".tmp");
        PathBuf::from(name)
    };
    let mut text = String::new();
    for row in &values {
        text.push_str(&serde_json::to_string(row).map_err(std::io::Error::other)?);
        text.push('\n');
    }
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn auto_row(
    at: DateTime<Utc>,
    line: &Line,
    settings: &rh_auto::Settings,
    report: Option<&ScanReport>,
    held_sec: u64,
    mode: Option<Mode>,
) -> Option<Value> {
    let candidate = rh_auto::candidate_of_with(line, settings, report)?;
    let held_enough = held_sec >= settings.hold_sec;
    let excluded = rh_auto::symbol_excluded(settings, &candidate.base);
    Some(row(
        at,
        "auto",
        &candidate.base,
        "USDT",
        candidate.long,
        candidate.short,
        candidate.entry_pct,
        candidate.net_to_normal_pct,
        Some(held_sec),
        Some(settings.min_net_pct),
        mode.map(|mode| match mode {
            Mode::Paper => "paper",
            Mode::Live => "live",
        }),
        held_enough && !excluded,
        auto_block(held_enough, excluded, &candidate, line),
    ))
}

fn auto_block(
    held_enough: bool,
    excluded: bool,
    candidate: &Candidate,
    line: &Line,
) -> Option<&'static str> {
    if excluded {
        return Some("在排除名单里，不做");
    }
    if !held_enough {
        return Some("信号保持时间还没到门槛");
    }
    if line.note.is_some() {
        return Some("这一行有备注，自动交易不会选");
    }
    let _ = candidate;
    None
}

fn row(
    at: DateTime<Utc>,
    kind: &str,
    base: &str,
    quote: &str,
    long: Venue,
    short: Venue,
    entry_pct: Decimal,
    net_pct: Decimal,
    held_sec: Option<u64>,
    min_net_pct: Option<Decimal>,
    mode: Option<&str>,
    openable: bool,
    block: Option<&str>,
) -> Value {
    json!({
        "at": at.to_rfc3339_opts(SecondsFormat::Secs, true),
        "kind": kind,
        "base": base,
        "quote": quote,
        "long": long.as_str(),
        "short": short.as_str(),
        "entry_pct": entry_pct.normalize().to_string(),
        "net_pct": net_pct.normalize().to_string(),
        "held_sec": held_sec,
        "min_net_pct": min_net_pct.map(|value| value.normalize().to_string()),
        "mode": mode,
        "openable": openable,
        "block": block,
    })
}

fn append(path: &Path, rows: &[Value]) -> bool {
    if rows.is_empty() {
        return false;
    }
    if let Err(error) = write(path, rows) {
        warn!(path = %path.display(), "可开仓记录没写上，不影响运行：{error}");
        return false;
    }
    true
}

fn write(path: &Path, rows: &[Value]) -> std::io::Result<()> {
    use std::io::Write;
    if let Some(dir) = path.parent()
        && !dir.as_os_str().is_empty()
    {
        std::fs::create_dir_all(dir)?;
    }
    let mut text = String::new();
    for row in rows {
        text.push_str(&serde_json::to_string(row).map_err(std::io::Error::other)?);
        text.push('\n');
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    file.write_all(text.as_bytes())?;
    Ok(())
}

/// 给测试看的一行。正式记录不读这个文件。
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Logged {
    kind: String,
    openable: bool,
}

#[cfg(test)]
mod tests {
    #[test]
    fn manual_push_uses_the_spread_alert_accounting() {
        use rust_decimal_macros::dec;
        let text = super::manual_push_text(
            "SPY",
            "lighter-rh",
            "arcus",
            dec!(300),
            dec!(3),
            dec!(0.184),
            dec!(-0.072),
            dec!(0.222),
            "周末",
            dec!(0.08),
            dec!(0.047),
        );
        assert!(
            text.contains("可成交价差 0.184%（300 USDT），周末正常基差 -0.072%，当前 0.222%（Arcus − Lighter RH）"),
            "{text}"
        );
        assert!(text.contains("回到正常净利润 0.08%（不含资金费，已扣手续费与平仓穿价）"), "{text}");
        assert!(
            text.contains("回到正常净利润 + 8 小时资金费 0.127%（资金费 0.047%，≈0.38 USDT）"),
            "{text}"
        );
    }

    #[test]
    fn manual_precheck_rejects_a_negative_return_to_normal() {
        use rust_decimal_macros::dec;
        let net = super::quote_net_to_normal(dec!(0.299), dec!(0.697));
        assert!(net < dec!(0.05));
        assert_eq!(net, dec!(-0.398));
        let cleared = super::quote_net_to_normal(dec!(0.055), dec!(-0.072));
        assert!(cleared >= dec!(0.05));
    }

    use super::*;
    use arb_core::Symbol;
    use arb_scanner::Opportunity;
    use rust_decimal_macros::dec;

    fn opportunity(entry: Decimal, net: Decimal, capped: bool) -> Opportunity {
        Opportunity {
            symbol: Symbol::perp("OPENAI", "USDT"),
            long: Venue::Binance,
            short: Venue::Gate,
            long_interval_h: 8,
            short_interval_h: 8,
            long_daily: Decimal::ZERO,
            short_daily: Decimal::ZERO,
            daily_spread: Decimal::ZERO,
            apr: Decimal::ZERO,
            round_trip_fee: dec!(0.002),
            round_trip_spread: Some(dec!(0.001)),
            round_trip_cost: dec!(0.003),
            spread_unknown: false,
            funding_daily: Decimal::ZERO,
            funding_apr: Decimal::ZERO,
            spread_net: net,
            spread_hold_days: Decimal::ONE,
            hold_measured: false,
            entry_basis_pct: Some(entry),
            executable_basis_pct: Some(entry),
            quote_mismatch: false,
            oi_capped: capped,
        }
    }

    #[test]
    fn manual_row_keeps_a_positive_spread_and_marks_the_oi_cap() {
        let row = manual_row(
            Utc::now(),
            &opportunity(dec!(0.4), dec!(0.002), false),
            None,
        )
        .unwrap();
        assert_eq!(row["kind"], "manual");
        assert_eq!(row["long"], "binance");
        // 还没有正常基差样本：价差为正也先记下，但不能标成达到门槛。
        assert_eq!(row["openable"], false);
        assert!(row["block"].as_str().unwrap().contains("样本不够"));

        assert!(manual_row(Utc::now(), &opportunity(dec!(0.4), dec!(0.002), true), None).is_none());
    }

    #[test]
    fn precheck_uses_its_own_floor_and_records_the_usdt_amount() {
        let opportunity = opportunity(dec!(1.0), dec!(0.01), false);
        let normal = Normal {
            session: session_of(),
            median: 0.2,
            p10: 0.1,
            p90: 0.3,
            mad: 0.05,
            minutes: MIN_MINUTES,
        };
        let mut normals = HashMap::new();
        normals.insert(
            basis_key(
                &opportunity.symbol.base,
                opportunity.long,
                opportunity.short,
                session_of(),
            ),
            normal,
        );
        let row = manual_row(Utc::now(), &opportunity, Some(&normals)).unwrap();
        let net: Decimal = row["net_to_normal_pct"].as_str().unwrap().parse().unwrap();
        // 一档净价差 1.0% − 正常基差 0.2%。测试里的资金费是 0。
        assert_eq!(net, dec!(0.8));
        assert!(net >= min_net_pct());
        assert_eq!(
            meets_precheck(&row),
            net >= precheck_min_net_pct().max(copied_settings().min_net_pct)
        );
        let size = copied_settings().size_usdt;
        let expected = (net / Decimal::ONE_HUNDRED * size).round_dp(4).normalize();
        let recorded: Decimal = row["net_to_normal_usdt"].as_str().unwrap().parse().unwrap();
        assert_eq!(recorded, expected);
        assert_eq!(row["size_usdt"], json!(size.normalize().to_string()));
        assert_eq!(
            row["precheck_min_net_pct"],
            json!(precheck_min_net_pct().normalize().to_string())
        );
        let mut below = row;
        below["price_net_pct"] = json!("0.1000");
        below["normal_basis_pct"] = json!("0.2000");
        below["net_to_normal_pct"] = json!("-0.1000");
        assert!(!meets_precheck(&below));
        // 旧行把资金费加进了回到正常：0.30 − 0.40 + 0.70 = 0.60。预检按 0.30 − 0.40 重算，不把 0.60 当成净利润。
        let mut stale = below.clone();
        stale["price_net_pct"] = json!("0.3000");
        stale["normal_basis_pct"] = json!("0.4000");
        stale["funding_pct"] = json!("0.7000");
        stale["net_to_normal_pct"] = json!("0.6000");
        assert!(!meets_precheck(&stale));
        let mut flat = below;
        flat["net_to_normal_pct"] = json!("1.0000");
        flat["entry_pct"] = json!("0");
        assert!(!meets_executable_spread(&flat));
        assert!(!meets_precheck(&flat));
        flat["entry_pct"] = json!("0.4000");
        flat["price_net_pct"] = json!("0");
        assert!(!meets_precheck(&flat));
    }

    #[test]
    fn manual_row_drops_when_the_one_tick_net_misses_the_floor() {
        let opportunity = opportunity(dec!(0.4), dec!(0.002), false);
        let normal = Normal {
            session: session_of(),
            median: 0.2,
            p10: 0.1,
            p90: 0.3,
            mad: 0.05,
            minutes: MIN_MINUTES,
        };
        let mut normals = HashMap::new();
        normals.insert(
            basis_key(
                &opportunity.symbol.base,
                opportunity.long,
                opportunity.short,
                session_of(),
            ),
            normal,
        );
        assert!(
            manual_row(Utc::now(), &opportunity, Some(&normals)).is_none(),
            "一档净价差 0.20% 减去正常基差 0.20% 后不够门槛，不进机会文件"
        );
    }

    #[test]
    fn manual_row_adds_eight_hour_funding() {
        // 价差净 0.30% − 正常基差 0.20% = 0.10%，先过 0.05% 门槛。资金费另记，不进净利润。
        let mut kept = opportunity(dec!(0.4), dec!(0.003), false);
        kept.daily_spread = dec!(0.01);
        let normal = Normal {
            session: session_of(),
            median: 0.2,
            p10: 0.1,
            p90: 0.3,
            mad: 0.05,
            minutes: MIN_MINUTES,
        };
        let mut normals = HashMap::new();
        normals.insert(
            basis_key(&kept.symbol.base, kept.long, kept.short, session_of()),
            normal,
        );
        let row = manual_row(Utc::now(), &kept, Some(&normals)).unwrap();
        let funding: Decimal = row["funding_pct"].as_str().unwrap().parse().unwrap();
        let expected_funding = (dec!(0.01) * dec!(8) / dec!(24) * dec!(100)).round_dp(4);
        assert_eq!(funding, expected_funding);
        let net: Decimal = row["net_to_normal_pct"].as_str().unwrap().parse().unwrap();
        assert_eq!(net, (dec!(0.3) - dec!(0.2)).round_dp(4));
        assert!(super::net_plus_funding_clears(net, funding));

        // 净利润不够 0.05%，资金费收入也不能把这笔写进机会文件。
        let mut thin = opportunity(dec!(0.4), dec!(0.002), false);
        thin.daily_spread = dec!(0.01);
        assert!(manual_row(Utc::now(), &thin, Some(&normals)).is_none());

        // 净利润过了门槛，但加上要付的资金费后不高于 0，也不写。
        let mut paying = opportunity(dec!(0.4), dec!(0.003), false);
        paying.daily_spread = dec!(-0.01);
        assert!(manual_row(Utc::now(), &paying, Some(&normals)).is_none());
    }

    #[test]
    fn manual_row_drops_a_spread_that_is_not_positive_after_costs() {
        assert!(
            manual_row(
                Utc::now(),
                &opportunity(dec!(0.4), Decimal::ZERO, false),
                None
            )
            .is_none()
        );
        assert!(
            manual_row(
                Utc::now(),
                &opportunity(Decimal::ZERO, dec!(0.002), false),
                None
            )
            .is_none()
        );
    }

    #[test]
    fn a_missing_file_is_created_and_failure_does_not_panic() {
        let dir = std::env::temp_dir().join(format!("opportunity-log-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("gone").join("opportunities.jsonl");
        append(&path, &[json!({"kind": "manual", "openable": true})]);
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("\"kind\":\"manual\""));
        std::fs::remove_file(&path).unwrap();
        append(&path, &[json!({"kind": "auto", "openable": true})]);
        assert!(std::fs::read_to_string(&path).unwrap().contains("auto"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn opportunity_log_is_off_unless_explicitly_enabled() {
        assert!(path_from_setting(None).is_none());
        assert!(path_from_setting(Some("")).is_none());
        assert!(path_from_setting(Some("off")).is_none());
        assert!(path_from_setting(Some("0")).is_none());
        assert!(path_from_setting(Some("false")).is_none());
        assert_eq!(
            path_from_setting(Some("on")).unwrap(),
            PathBuf::from(DEFAULT_PATH)
        );
        assert_eq!(
            path_from_setting(Some("true")).unwrap(),
            PathBuf::from(DEFAULT_PATH)
        );
        assert_eq!(
            path_from_setting(Some(" custom/opportunities.jsonl ")).unwrap(),
            PathBuf::from("custom/opportunities.jsonl")
        );
    }

    #[test]
    fn manual_flow_continues_when_the_opportunity_file_is_off() {
        let dir = std::env::temp_dir().join(format!("opportunity-memory-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let basis = dir.join("opportunity-log-basis.jsonl");
        let session = session_of();
        let now = Utc::now().timestamp() / 60 * 60;
        let seeded: Vec<BasisSample> = (0..MIN_MINUTES as i64)
            .map(|back| BasisSample {
                t: now - (back + 1) * 60,
                base: "FLOW".into(),
                long: "binance".into(),
                short: "gate".into(),
                session,
                basis: 0.40,
            })
            .collect();
        append_basis(&basis, &seeded);
        let report = ScanReport {
            generated_at: Utc::now(),
            fee_per_side: dec!(0.0005),
            amortize_days: dec!(7),
            spread_hold_days: Decimal::ONE,
            max_entry_basis_pct: None,
            min_venues: 2,
            venues: Vec::new(),
            symbols: vec![view_of(opportunity_named(
                "FLOW",
                dec!(2.0),
                dec!(0.01),
                false,
            ))],
            unverified: Vec::new(),
            suspicious: Vec::new(),
            gated: Vec::new(),
            unpaired: Vec::new(),
            totals: Default::default(),
        };
        record_at(None, &basis, &report);
        record_at(None, &basis, &report);
        assert!(
            !dir.join("opportunity-log.jsonl").exists(),
            "机会文件关掉时不该写机会行"
        );
        let rows = load_active(&active_path_for(&basis));
        let row = rows
            .iter()
            .find(|row| row["base"] == "FLOW")
            .expect("达标的手动机会要留在活跃文件里");
        assert_eq!(row["held_rounds"], 2);
        assert_eq!(row["openable"], true);
        let active = std::fs::read_to_string(active_path_for(&basis)).unwrap();
        assert!(active.contains("FLOW"));
        let loaded = load_basis(&basis).unwrap();
        assert_eq!(
            loaded.iter().filter(|sample| sample.base == "FLOW").count(),
            MIN_MINUTES + 1
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn active_rows_leave_when_they_miss_and_a_push_lasts_one_hour() {
        let now = Utc::now();
        let mut row = json!({
            "kind": "manual",
            "openable": true,
            "held_rounds": 2,
            "base": "KEEP",
            "long": "binance",
            "short": "gate",
            "entry_pct": "1",
            "price_net_pct": "0.2000",
            "net_to_normal_pct": "1.0000"
        });
        assert!(
            merge_active(&[row.clone()], &[], now).is_empty(),
            "没推送过、这一轮不达标就删"
        );
        row["pushed_at"] = json!(now.to_rfc3339_opts(SecondsFormat::Secs, true));
        assert_eq!(merge_active(&[row.clone()], &[], now).len(), 1);
        row["pushed_at"] =
            json!((now - chrono::Duration::hours(2)).to_rfc3339_opts(SecondsFormat::Secs, true));
        assert!(
            merge_active(&[row.clone()], &[], now).is_empty(),
            "推送超过 1 小时就删"
        );
        row["pushed_at"] = json!(now.to_rfc3339_opts(SecondsFormat::Secs, true));
        let kept = merge_active(&[row.clone()], &[row.clone()], now);
        assert_eq!(kept[0]["pushed_at"], row["pushed_at"]);
    }

    #[test]
    fn basis_file_drops_samples_older_than_seven_days() {
        let dir =
            std::env::temp_dir().join(format!("opportunity-basis-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let basis = basis_path(&dir.join("opportunity-log.jsonl"));
        let old = BasisSample {
            t: Utc::now().timestamp() - 8 * 24 * 3600,
            base: "OLD".into(),
            long: "binance".into(),
            short: "gate".into(),
            session: Session::Rth,
            basis: 1.5,
        };
        let recent = BasisSample {
            t: Utc::now().timestamp() - 3600,
            base: "NEW".into(),
            long: "binance".into(),
            short: "gate".into(),
            session: Session::Rth,
            basis: 0.4,
        };
        append_basis(&basis, &[old]);
        append_basis(&basis, &[recent]);
        let text = std::fs::read_to_string(&basis).unwrap();
        assert!(!text.contains("OLD"), "超出 7 天的样本不该留在文件里");
        assert!(text.contains("NEW"));
        let loaded = load_basis(&basis).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].base, "NEW");
        assert!((loaded[0].basis - 0.4).abs() < 1e-9);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn basis_samples_only_pairs_that_enter_the_manual_file() {
        let dir =
            std::env::temp_dir().join(format!("opportunity-basis-filter-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("opportunity-log.jsonl");
        let basis = basis_path(&path);
        let session = session_of();
        let now = Utc::now().timestamp() / 60 * 60;
        let seeded: Vec<BasisSample> = (0..MIN_MINUTES as i64)
            .map(|back| BasisSample {
                t: now - back * 60,
                base: "LOW".into(),
                long: "binance".into(),
                short: "gate".into(),
                session,
                basis: 0.40,
            })
            .collect();
        append_basis(&basis, &seeded);

        let report = ScanReport {
            generated_at: Utc::now(),
            fee_per_side: dec!(0.0005),
            amortize_days: dec!(7),
            spread_hold_days: Decimal::ONE,
            max_entry_basis_pct: None,
            min_venues: 2,
            venues: Vec::new(),
            symbols: vec![
                view_of(opportunity_named("KEEP", dec!(0.4), dec!(0.002), false)),
                view_of(opportunity_named("LOW", dec!(0.4), dec!(0.002), false)),
                view_of(opportunity_named("CAP", dec!(0.4), dec!(0.002), true)),
            ],
            unverified: Vec::new(),
            suspicious: Vec::new(),
            gated: Vec::new(),
            unpaired: Vec::new(),
            totals: Default::default(),
        };
        record_enabled_scan(Some(&path), &report);
        record_enabled_scan(Some(&path), &report);

        let opportunities = std::fs::read_to_string(&path).unwrap();
        assert!(opportunities.contains("KEEP"));
        assert!(!opportunities.contains("LOW"));
        assert!(!opportunities.contains("CAP"));
        let loaded = load_basis(&basis).unwrap();
        assert!(loaded.iter().all(|sample| sample.base != "CAP"));
        assert_eq!(
            loaded.iter().filter(|sample| sample.base == "KEEP").count(),
            1,
            "同一分钟重复扫描不该再记一条"
        );
        assert_eq!(
            loaded.iter().filter(|sample| sample.base == "LOW").count(),
            MIN_MINUTES,
            "回到正常的净收益不够时，不再继续采样"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn another_session_does_not_replace_the_current_normal() {
        let now = Utc::now();
        let current = session_of();
        let other = if current == Session::Rth {
            Session::Off
        } else {
            Session::Rth
        };
        let sample = |session, basis| BasisSample {
            t: now.timestamp(),
            base: "NVDA".into(),
            long: "arcus".into(),
            short: "lighter-rh".into(),
            session,
            basis,
        };
        let mut samples = Vec::new();
        for _ in 0..MIN_MINUTES {
            samples.push(sample(current, 0.20));
            samples.push(sample(other, 1.50));
        }
        let normals = normals_of(&samples, now);
        let current_normal = &normals[&format!("NVDA|arcus|lighter-rh|{}", session_key(current))];
        let other_normal = &normals[&format!("NVDA|arcus|lighter-rh|{}", session_key(other))];
        assert!((current_normal.median - 0.20).abs() < 1e-9);
        assert!((other_normal.median - 1.50).abs() < 1e-9);
        assert_eq!(current_normal.session, current);
    }

    #[test]
    fn close_target_uses_the_recorded_session_median() {
        let mut low = vec![0.0; 60];
        low.extend(vec![1.0; 60]);
        assert_eq!(position_target(low).unwrap(), dec!(0.5));
        assert!(position_target(vec![0.2; MIN_MINUTES - 1]).is_err());
        assert!(position_target(vec![6.0; MIN_MINUTES]).is_err());
    }

    fn opportunity_named(base: &str, entry: Decimal, net: Decimal, capped: bool) -> Opportunity {
        let mut opportunity = opportunity(entry, net, capped);
        opportunity.symbol = Symbol::perp(base, "USDT");
        opportunity
    }

    fn view_of(opportunity: Opportunity) -> arb_scanner::SymbolView {
        arb_scanner::SymbolView {
            symbol: opportunity.symbol.clone(),
            rates: Vec::new(),
            funding: Vec::new(),
            spread: vec![opportunity],
        }
    }
}
