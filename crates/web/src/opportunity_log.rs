//! 可开仓机会的旁路记录。
//!
//! 只追加到单独文件，不参与扫描、排名、闸门或自动开仓。文件缺失、建不了目录、
//! 写失败都只记一条日志，调用方照常继续。删掉文件不影响脚本运行。

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
        warn!("{name} 不是数字，手动交易净收益门槛用 {fallback}");
        return fallback;
    };
    if value < Decimal::ZERO || value > Decimal::from(20u32) {
        warn!("{name} 必须在 0 到 20 之间，手动交易净收益门槛用 {fallback}");
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

static MIN_NET_PCT: LazyLock<Decimal> = LazyLock::new(configured_min_net_pct);
static PRECHECK_MIN_NET_PCT: LazyLock<Decimal> = LazyLock::new(configured_precheck_min_net_pct);

/// 手动信号已连续出现的扫描轮数。中断或从榜上消失就清零。
static MANUAL_ROUNDS: LazyLock<Mutex<HashMap<String, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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

/// 这一行是否达到进预检的回到正常净收益门槛。缺数字不算达标。
/// 同时还要符合自动交易的可成交价差标准：价差为正，扣掉平仓穿价和往返手续费后仍为正，
/// 且回到正常的净收益不低于自动交易面板上的门槛。
pub fn meets_precheck(row: &Value) -> bool {
    if !meets_executable_spread(row) {
        return false;
    }
    let Some(net) = row_pct(row, "net_to_normal_pct") else {
        return false;
    };
    net >= precheck_floor()
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

fn precheck_floor() -> Decimal {
    precheck_min_net_pct().max(copied_settings().min_net_pct)
}

fn row_pct(row: &Value, field: &str) -> Option<Decimal> {
    row[field]
        .as_str()
        .and_then(|text| text.parse::<Decimal>().ok())
}

/// 推送文案。价差数字来自机会记录；`depth_net_pct` 是预检吃完深度后的净价差（%）。
pub fn manual_push_text(
    base: &str,
    long: &str,
    short: &str,
    size: Decimal,
    leverage: Decimal,
    entry_pct: Decimal,
    price_net_pct: Decimal,
    net_to_normal_pct: Decimal,
    depth_net_pct: Option<Decimal>,
) -> String {
    let profit_usdt = (net_to_normal_pct / Decimal::ONE_HUNDRED * size)
        .round_dp(2)
        .normalize();
    let depth = depth_net_pct.map_or_else(String::new, |net| {
        format!("吃完深度后净价差 {}%。", net.normalize())
    });
    format!(
        "手动机会已通过预检：{base} 多 {long} / 空 {short}，{} USDT × {}x。可成交价差 {}%，扣费后净 {}%。{depth}回到正常预估盈利 {}%（约 {profit_usdt} USDT）。30 分钟内有效，用过即废。下单前仍按自动开仓标准重查。",
        size.normalize(),
        leverage.normalize(),
        entry_pct.normalize(),
        price_net_pct.normalize(),
        net_to_normal_pct.normalize(),
        profit_usdt = profit_usdt,
    )
}

/// 最近一轮里、已经达到预检标准的这组手动机会的价差。
pub struct ManualSpread {
    pub entry_pct: Decimal,
    pub price_net_pct: Decimal,
    pub net_to_normal_pct: Decimal,
}

pub fn spread_for_push(
    path: Option<&Path>,
    base: &str,
    long: Venue,
    short: Venue,
) -> Option<ManualSpread> {
    qualifying_manual(path).into_iter().find_map(|row| {
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
            price_net_pct: row_pct(&row, "price_net_pct")?,
            net_to_normal_pct: row_pct(&row, "net_to_normal_pct")?,
        })
    })
}

/// 正常基差样本文件。跟机会记录放在同一目录，文件名加 `-basis`。
/// 删掉它只是重新攒样本，不影响开仓。
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
    path.with_file_name(basis)
}

/// 记录文件路径。`ARB_OPPORTUNITY_LOG=off` 关闭；不设则写到当前目录的单独文件。
/// 最近一轮里某个手动机会的回到正常净收益（%）。没有或过期则为空。
pub fn manual_net_to_normal(
    path: Option<&Path>,
    base: &str,
    long: Venue,
    short: Venue,
) -> Option<String> {
    qualifying_manual(path).into_iter().find_map(|row| {
        (row["base"] == base && row["long"] == long.as_str() && row["short"] == short.as_str())
            .then(|| row["net_to_normal_pct"].as_str().map(str::to_string))
            .flatten()
    })
}

/// 最近一轮里，已经算出回到正常净收益且达到页面门槛的手动机会。
pub fn qualifying_manual(path: Option<&Path>) -> Vec<Value> {
    if !enabled() {
        return Vec::new();
    }
    let Some(path) = path else {
        return Vec::new();
    };
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut latest: HashMap<String, Value> = HashMap::new();
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value["kind"] != "manual" || value["openable"] != true {
            continue;
        }
        let Ok(at) = value["at"]
            .as_str()
            .unwrap_or_default()
            .parse::<DateTime<Utc>>()
        else {
            continue;
        };
        if Utc::now().signed_duration_since(at) > chrono::Duration::minutes(3) {
            continue;
        }
        let key = format!(
            "{}|{}|{}",
            value["base"].as_str().unwrap_or_default(),
            value["long"].as_str().unwrap_or_default(),
            value["short"].as_str().unwrap_or_default()
        );
        latest.insert(key, value);
    }
    let mut rows: Vec<Value> = latest.into_values().collect();
    rows.sort_by(|left, right| {
        right["net_to_normal_pct"]
            .as_str()
            .unwrap_or_default()
            .cmp(left["net_to_normal_pct"].as_str().unwrap_or_default())
    });
    rows.truncate(MANUAL_TOP);
    rows
}

pub fn path_from_env() -> Option<PathBuf> {
    match std::env::var("ARB_OPPORTUNITY_LOG") {
        Ok(raw) if raw.trim().eq_ignore_ascii_case("off") => None,
        Ok(raw) if !raw.trim().is_empty() => Some(PathBuf::from(raw.trim())),
        _ => Some(PathBuf::from(DEFAULT_PATH)),
    }
}

/// 扫描榜上可成交价差为正、扣费后仍为正的配对。手动开仓看的就是这一档。
pub fn record_scan(path: Option<&Path>, report: &ScanReport) {
    if !enabled() {
        return;
    }
    let Some(path) = path else {
        return;
    };
    let at = report.generated_at;
    let normals = note_basis(path, report);
    let mut rows = Vec::new();
    let mut still = HashSet::new();
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
        let rounds = manual_rounds(&key);
        row["held_rounds"] = json!(rounds);
        row["hold_rounds"] = json!(MANUAL_HOLD_ROUNDS);
        rows.push(row);
    }
    if let Ok(mut seen) = MANUAL_ROUNDS.lock() {
        seen.retain(|key, _| still.contains(key));
    }
    append(path, &rows);
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
/// 有正常基差时比回到正常的净收益；没有时比价差净收益加持有期资金费。
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
    let funding = funding_over_hold_pct(opportunity).unwrap_or(Decimal::ZERO);
    let price = (opportunity.spread_net * Decimal::ONE_HUNDRED).round_dp(4);
    let Some(normals) = normals else {
        return price + funding;
    };
    let key = format!(
        "{}|{}|{}",
        opportunity.symbol.base,
        opportunity.long.as_str(),
        opportunity.short.as_str()
    );
    let Some(normal) = normals.get(&key) else {
        return price + funding;
    };
    let Some(mark) = opportunity.entry_basis_pct else {
        return price + funding;
    };
    let Some(mark) = mark.to_f64() else {
        return price + funding;
    };
    Decimal::try_from(mark - normal.median)
        .map(|back| (back + funding).round_dp(4))
        .unwrap_or(price + funding)
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

/// 价差监控里已经达到自动开仓条件的一行。没保持够、不在名单里的不记。
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
    );
    let normal = normals.and_then(|map| map.get(&key));
    let price_net_pct = (opportunity.spread_net * Decimal::ONE_HUNDRED).round_dp(4);
    let funding_pct = funding_over_hold_pct(opportunity);
    let net_pct = match funding_pct {
        Some(funding) => (price_net_pct + funding).round_dp(4),
        None => price_net_pct,
    };
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
        value["funding_note"] = json!("结算周期无效，资金费没算进净收益");
    }
    attach_normal(&mut value, normal, opportunity.entry_basis_pct);
    let min_net = min_net_pct();
    let precheck_min = precheck_min_net_pct();
    let net = value["net_to_normal_pct"]
        .as_str()
        .and_then(|text| text.parse::<Decimal>().ok());
    let meets = net.is_some_and(|net| net >= min_net);
    value["min_net_pct"] = json!(min_net.normalize().to_string());
    value["precheck_min_net_pct"] = json!(precheck_min.normalize().to_string());
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

/// 这一轮扫描的标记价基差记一分钟。样本不够时不编正常值。
fn note_basis(path: &Path, report: &ScanReport) -> Option<HashMap<String, Normal>> {
    let minute = report.generated_at.timestamp() / 60 * 60;
    let session = session_of();
    let mut fresh = Vec::new();
    for (view, opportunity) in top_spread(report, None, MANUAL_TOP) {
        {
            let Some(basis) = opportunity.entry_basis_pct.and_then(|basis| basis.to_f64()) else {
                continue;
            };
            if !basis.is_finite() {
                continue;
            }
            fresh.push(BasisSample {
                t: minute,
                base: view.symbol.base.clone(),
                long: opportunity.long.as_str().to_string(),
                short: opportunity.short.as_str().to_string(),
                session,
                basis,
            });
        }
    }
    let file = basis_path(path);
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
        ))
    });
    if !fresh.is_empty() {
        append_basis(&file, &fresh);
    }
    match load_basis(&file) {
        Ok(samples) => Some(normals_of(&samples, report.generated_at)),
        Err(error) => {
            warn!(path = %file.display(), "正常基差算不出来，机会行里留空：{error}");
            None
        }
    }
}

fn recent_minutes(
    path: &Path,
    minute: i64,
) -> std::io::Result<HashSet<(i64, String, String, String)>> {
    let samples = load_basis(path)?;
    Ok(samples
        .into_iter()
        .filter(|sample| sample.t == minute)
        .map(|sample| (sample.t, sample.base, sample.long, sample.short))
        .collect())
}

fn session_of() -> Session {
    // 扫描快照没有「是不是加密币」这一项。按纽约时段分，不把股票误标成全天。
    session::classify(Utc::now(), false, None)
}

fn basis_key(base: &str, long: Venue, short: Venue) -> String {
    format!("{base}|{}|{}", long.as_str(), short.as_str())
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
            format!("{base}|{long}|{short}"),
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

/// 按价差持有天数估算这一笔会收付的资金费（%）。
/// 方向与价差单相同：空腿收、多腿付。日化费差来自扫描；持有天数用机会页的「价差持有」。
fn funding_over_hold_pct(opportunity: &Opportunity) -> Option<Decimal> {
    if opportunity.long_interval_h == 0 || opportunity.short_interval_h == 0 {
        return None;
    }
    let days = if opportunity.spread_hold_days > Decimal::ZERO {
        opportunity.spread_hold_days
    } else {
        Decimal::ONE
    };
    let pct = opportunity.daily_spread * days * Decimal::ONE_HUNDRED;
    Some(pct.round_dp(4))
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
    value["normal_minutes"] = json!(normal.minutes);
    let price = mark_basis.and_then(|basis| basis.to_f64()).map(|basis| {
        // 标记价基差已经是（空 − 多）。回到中位数时，多出来的那一段才是能收回的。
        basis - normal.median
    });
    let funding = value["funding_pct"]
        .as_str()
        .and_then(|text| text.parse::<f64>().ok());
    value["net_to_normal_pct"] = match (price, funding) {
        (Some(price), Some(funding)) => json!(format!("{:.4}", price + funding)),
        (Some(price), None) => json!(format!("{price:.4}")),
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
    let listed = settings.symbols.is_empty() || settings.symbols.contains(&candidate.base);
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
        held_enough && listed,
        auto_block(held_enough, listed, &candidate, line),
    ))
}

fn auto_block(
    held_enough: bool,
    listed: bool,
    candidate: &Candidate,
    line: &Line,
) -> Option<&'static str> {
    if !listed {
        return Some("不在自动交易的合约名单里");
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

fn append(path: &Path, rows: &[Value]) {
    if rows.is_empty() {
        return;
    }
    if let Err(error) = write(path, rows) {
        warn!(path = %path.display(), "可开仓记录没写上，不影响运行：{error}");
    }
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
            ),
            normal,
        );
        let row = manual_row(Utc::now(), &opportunity, Some(&normals)).unwrap();
        let net: Decimal = row["net_to_normal_pct"].as_str().unwrap().parse().unwrap();
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
        below["net_to_normal_pct"] = json!("-0.0100");
        assert!(!meets_precheck(&below));
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
}
