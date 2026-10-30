//! 价差自动交易：价差监控（全部组：用户配了 API 的场所两两组合）出信号后，按用户在面板上设定的参数自动开价差单。
//! 两条腿都必须是实盘已连接的场所（纸面模式不限）；规则与最早只做 Arcus ↔ Lighter RH 时完全相同。
//!
//! **不另起一条下单路径**：每一笔都走页面下单同一个函数（[`crate::trade::open_for_auto`] →
//! `run_open`），所以对账、按现拉盘口重算深度与净收益、单笔上限、持仓数上限、当日亏损闸门、
//! Telegram `/pause`、连续失败熔断、开仓通知一个不少。这里只多加自动交易自己的限制：
//!
//! - 默认关闭；开启实盘要在请求里显式确认（`confirm = "LIVE"`）；
//! - 同一合约已有敞口就不再开（不论是不是自动开的）；
//! - 自动开的仓位同时最多 `max_positions` 笔，每个 UTC 日最多 `daily_max_opens` 笔；
//! - 每次尝试后该合约冷却；两次开仓之间至少隔 [`MIN_OPEN_GAP`]；
//! - 必须至少带一条退出规则（回到正常基差平仓 / 止盈）：没有退出条件的自动仓位只会一直挂着；
//! - 资金费核算默认关闭。开启后，开仓后 3 或 6 小时要付的资金费（空腿收、多腿付）加上往返手续费，大于等于回到正常的预估利润时不合格，不进入下单检查；
//! - 执行中断（结果未知）立刻关闭自动交易；回滚没完成、仓位仍在 `unwinding` 也立刻关闭，
//!   不计入连续回滚次数；开仓连续以回滚收场 [`MAX_FAILURES`] 次也关闭。
//!
//! 设置与「自动开过哪些仓」写在价差数据目录的 `auto.json`（0600，原子替换），重启后照旧。

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arb_core::{Decimal, Venue};
use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::AppState;
use crate::rh_spread::{Line, View};
use crate::trade::Mode;

/// 同一合约尝试一次后的冷却（成功或被闸门拒绝）。
const SYMBOL_COOLDOWN: Duration = Duration::from_secs(10 * 60);
/// 被闸门拒绝后，下一次尝试（任何合约）至少隔这么久。
const REJECT_GAP: Duration = Duration::from_secs(15);
/// 交易台忙（规则轮或手动操作占着锁）：稍后再试。
const BUSY_RETRY: Duration = Duration::from_secs(5);
/// 两次自动开仓之间至少隔这么久。
pub const MIN_OPEN_GAP: Duration = Duration::from_secs(60);

/// `ARB_RH_AUTO_OPEN_GAP_SEC`：不填或空 = [`MIN_OPEN_GAP`]；0 = 不等待；大于 3600 或不是整数则回落默认。
fn open_gap_from_env() -> Duration {
    match std::env::var("ARB_RH_AUTO_OPEN_GAP_SEC") {
        Ok(raw) if !raw.trim().is_empty() => match raw.trim().parse::<u64>() {
            Ok(sec) if sec <= 3600 => Duration::from_secs(sec),
            Ok(sec) => {
                warn!(sec, "ARB_RH_AUTO_OPEN_GAP_SEC 超过 3600，按 60 秒");
                MIN_OPEN_GAP
            }
            Err(_) => {
                warn!(raw = %raw.trim(), "ARB_RH_AUTO_OPEN_GAP_SEC 必须是 0 到 3600 的整数，按 60 秒");
                MIN_OPEN_GAP
            }
        },
        _ => MIN_OPEN_GAP,
    }
}
/// 开仓连续以回滚收场这么多次就关闭自动交易。
pub const MAX_FAILURES: u32 = 2;
/// 价差监控的快照超过这么久没更新就不下单。
const VIEW_STALE_SEC: i64 = 5;
/// 页面上保留的最近事件条数。
const EVENTS: usize = 60;
/// `auto.json` 里保留的自动开仓记录条数。
const OPENED_KEEP: usize = 300;

/// 用户在面板上设定的参数。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Settings {
    pub enabled: bool,
    pub mode: Mode,
    /// 单腿名义（USDT）。
    pub size_usdt: Decimal,
    /// 杠杆（两腿相同，整数）。
    pub leverage: Decimal,
    /// 触发门槛：回到正常基差的预估净收益（%）。
    pub min_net_pct: Decimal,
    /// 信号要连续保持多少秒。
    pub hold_sec: u64,
    /// 回到同时段正常基差就平仓（基差收敛规则，目标按方向换算）。
    pub back_to_normal: bool,
    /// 止盈（USDT，含资金费，按盘口核对）。
    pub take_profit_usdt: Option<Decimal>,
    /// 爆仓保护（强平距离 %，逐仓）。
    pub liq_protection_pct: Option<Decimal>,
    /// 自动追加保证金的触发强平距离（%）。与上限成对；缺一或都缺表示关闭。
    #[serde(default)]
    pub auto_margin_pct: Option<Decimal>,
    /// 自动追加保证金的累计上限（USDT）。
    #[serde(default)]
    pub auto_margin_max_usdt: Option<Decimal>,
    /// 自动开的仓位同时最多几笔。
    pub max_positions: usize,
    /// 每个 UTC 日最多自动开几笔。
    pub daily_max_opens: usize,
    /// 只做这些合约（base，大写）；空 = 全部。
    #[serde(default)]
    pub symbols: Vec<String>,
    /// 开仓后资金费核算小时。`None` 关闭；只允许 3 或 6。
    #[serde(default)]
    pub funding_check_hours: Option<u32>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: Mode::Paper,
            size_usdt: Decimal::from(500),
            leverage: Decimal::from(3),
            min_net_pct: Decimal::new(5, 2),
            hold_sec: 10,
            back_to_normal: true,
            take_profit_usdt: None,
            liq_protection_pct: None,
            auto_margin_pct: None,
            auto_margin_max_usdt: None,
            max_positions: 1,
            daily_max_opens: 3,
            symbols: Vec::new(),
            funding_check_hours: None,
        }
    }
}

impl Settings {
    /// 校验。`max_size` 是 `ARB_MAX_POSITION_USDT`（没设上限为 `None`）。
    pub fn validate(&self, max_size: Option<Decimal>) -> Result<(), String> {
        if self.size_usdt < Decimal::from(10) || self.size_usdt > Decimal::from(100_000) {
            return Err("单腿名义必须在 10 到 100000 USDT 之间".into());
        }
        if let Some(max) = max_size
            && self.size_usdt > max
        {
            return Err(format!(
                "单腿名义 {} 超过单笔上限 ARB_MAX_POSITION_USDT={}",
                self.size_usdt.normalize(),
                max.normalize()
            ));
        }
        if !self.leverage.fract().is_zero()
            || self.leverage < Decimal::ONE
            || self.leverage > Decimal::from(10)
        {
            return Err("杠杆必须是 1 到 10 的整数".into());
        }
        if self.min_net_pct < Decimal::new(1, 2) || self.min_net_pct > Decimal::from(5) {
            return Err("触发门槛必须在 0.01% 到 5% 之间".into());
        }
        if !(5..=600).contains(&self.hold_sec) {
            return Err("信号保持时间必须在 5 到 600 秒之间".into());
        }
        if !self.back_to_normal && self.take_profit_usdt.is_none() {
            return Err(
                "至少开启一条退出规则（回到正常基差平仓 / 止盈）：没有退出条件的自动仓位只会一直挂着"
                    .into(),
            );
        }
        if let Some(tp) = self.take_profit_usdt
            && (tp <= Decimal::ZERO || tp > Decimal::from(100_000))
        {
            return Err("止盈必须大于 0".into());
        }
        if let Some(pct) = self.liq_protection_pct
            && (pct <= Decimal::ZERO || pct >= Decimal::from(100))
        {
            return Err("爆仓保护必须在 0 到 100% 之间".into());
        }
        match (self.auto_margin_pct, self.auto_margin_max_usdt) {
            (None, None) => {}
            (Some(pct), Some(max)) => {
                if pct <= Decimal::ZERO || pct >= Decimal::from(100) {
                    return Err("自动追加保证金的触发线必须在 0 到 100% 之间".into());
                }
                if max <= Decimal::ZERO || max > Decimal::from(1_000_000) {
                    return Err("自动追加保证金的累计上限必须在 0 到 1000000 USDT 之间".into());
                }
                if let Some(protection) = self.liq_protection_pct
                    && pct <= protection
                {
                    return Err(format!(
                        "自动追加保证金的触发线 {}% 必须高于爆仓保护 {}%",
                        pct.normalize(),
                        protection.normalize()
                    ));
                }
            }
            _ => {
                return Err("自动追加保证金的触发线和累计上限必须同时设置或同时关闭".into());
            }
        }
        if !(1..=30).contains(&self.max_positions) {
            return Err("同时持仓数必须在 1 到 30 之间".into());
        }
        if !(1..=50).contains(&self.daily_max_opens) {
            return Err("每日开仓数必须在 1 到 50 之间".into());
        }
        if self.symbols.len() > 100
            || self.symbols.iter().any(|s| {
                s.is_empty() || s.len() > 20 || !s.chars().all(|c| c.is_ascii_alphanumeric())
            })
        {
            return Err("合约名单只能是字母数字，逗号分隔".into());
        }
        if self
            .funding_check_hours
            .is_some_and(|hours| hours != 3 && hours != 6)
        {
            return Err("资金费核算只能关闭，或选择 3 小时、6 小时".into());
        }
        Ok(())
    }
}

/// 一笔自动开出来的仓位。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Opened {
    pub id: String,
    pub symbol: String,
    pub mode: Mode,
    pub at: DateTime<Utc>,
}

/// 写在 `auto.json` 里的全部状态。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Persisted {
    #[serde(default)]
    settings: Settings,
    #[serde(default)]
    opened: Vec<Opened>,
    /// 连续以回滚收场的次数（成功一次清零）。
    #[serde(default)]
    failures: u32,
    /// 最近一次被自动关闭的原因。
    #[serde(default)]
    disabled_reason: Option<String>,
}

/// 一条事件（页面上的「最近动作」）。
#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub at: DateTime<Utc>,
    pub kind: &'static str,
    pub symbol: Option<String>,
    pub text: String,
}

/// 选中的一笔：方向、目标、信号上的数字。
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub base: String,
    pub long: Venue,
    pub short: Venue,
    /// 基差收敛目标（%，持仓口径：空腿 − 多腿）。
    pub target_pct: Option<Decimal>,
    pub entry_pct: Decimal,
    pub net_to_normal_pct: Decimal,
    pub signal_sec: u64,
}

/// 计时的键：同一个合约可能出现在几组里（同一个多腿、不同的空腿），要分开计。
pub type HoldKey = (String, Venue, Venue);

impl Candidate {
    pub fn hold_key(&self) -> HoldKey {
        (self.base.clone(), self.long, self.short)
    }
}

/// 价差页基差是 (a − b)；持仓规则的基差是 (空 − 多)。多 a / 空 b 时持仓基差 = −页面基差，
/// 所以「回到正常」的目标 = −中位数；反方向就是中位数本身。超出规则允许的 ±5% 时不给目标。
pub fn target_for(direction: &str, median: f64) -> Option<Decimal> {
    let raw = if direction == "long_a" {
        -median
    } else {
        median
    };
    let value = Decimal::from_f64_retain(raw)?.round_dp(3);
    (value.abs() <= arb_exec::monitor::MAX_BASIS_EXIT_PCT).then_some(value)
}

/// 更新「达到门槛」的连续计时：只保留这一刻仍达标的（合约, 方向），返回各自已保持的秒数。
/// 用自己的门槛计时，不用监控的提醒信号 —— 用户的门槛可能低于提醒门槛。
pub fn track_holds(
    view: &View,
    settings: &Settings,
    since: &mut HashMap<HoldKey, Instant>,
    now: Instant,
) {
    track_holds_with(view, settings, None, since, now);
}

pub fn track_holds_with(
    view: &View,
    settings: &Settings,
    report: Option<&arb_scanner::ScanReport>,
    since: &mut HashMap<HoldKey, Instant>,
    now: Instant,
) {
    let current: Vec<HoldKey> = view
        .lines
        .iter()
        .filter_map(|line| candidate_of_with(line, settings, report).map(|c| c.hold_key()))
        .collect();
    since.retain(|key, _| current.contains(key));
    for key in current {
        since.entry(key).or_insert(now);
    }
}

/// 从价差监控的快照里挑一笔。`Err` 是这一刻为什么不下单（给页面看）。
/// `tradable`：这家场所能不能下单（实盘：已连接；纸面：都能）。两条腿都要能下单的组才做。
pub fn pick(
    view: &View,
    settings: &Settings,
    now: DateTime<Utc>,
    tradable: &dyn Fn(Venue) -> bool,
    held_sec: &dyn Fn(&Candidate) -> u64,
    busy_symbols: &dyn Fn(&str) -> Option<String>,
) -> Result<Candidate, String> {
    pick_with(view, settings, None, now, tradable, held_sec, busy_symbols)
}

pub fn pick_with(
    view: &View,
    settings: &Settings,
    report: Option<&arb_scanner::ScanReport>,
    now: DateTime<Utc>,
    tradable: &dyn Fn(Venue) -> bool,
    held_sec: &dyn Fn(&Candidate) -> u64,
    busy_symbols: &dyn Fn(&str) -> Option<String>,
) -> Result<Candidate, String> {
    match view.updated_at {
        Some(at) if (now - at).num_seconds() <= VIEW_STALE_SEC => {}
        _ => return Err("价差监控的快照不新鲜".into()),
    }
    let mut waiting: Option<String> = None;
    let mut blocked: Option<String> = None;
    for line in &view.lines {
        let Some(mut candidate) = candidate_of_with(line, settings, report) else {
            continue;
        };
        // 行情断了的那家，盘口会被清空、这一行不会有报价；这里再挡一次「行情连着但账户没连」的。
        if !tradable(candidate.long) || !tradable(candidate.short) {
            continue;
        }
        if !view.connected.up(candidate.long) || !view.connected.up(candidate.short) {
            continue;
        }
        candidate.signal_sec = held_sec(&candidate);
        if !settings.symbols.is_empty() && !settings.symbols.contains(&candidate.base) {
            continue;
        }
        if candidate.signal_sec < settings.hold_sec {
            waiting.get_or_insert(format!(
                "{} 信号已保持 {}/{} 秒",
                candidate.base, candidate.signal_sec, settings.hold_sec
            ));
            continue;
        }
        if let Some(why) = busy_symbols(&candidate.base) {
            blocked.get_or_insert(format!("{}：{why}", candidate.base));
            continue;
        }
        return Ok(candidate);
    }
    Err(waiting
        .or(blocked)
        .unwrap_or_else(|| "没有达到门槛的信号".into()))
}

/// 一行能不能做、怎么做。没有扫描快照时不算资金费，只给不碰行情的测试用。
pub fn candidate_of(line: &Line, settings: &Settings) -> Option<Candidate> {
    candidate_of_with(line, settings, None)
}

/// 带扫描快照的筛选。开启资金费核算时，没有快照或对不上费率就不合格。
pub fn candidate_of_with(
    line: &Line,
    settings: &Settings,
    report: Option<&arb_scanner::ScanReport>,
) -> Option<Candidate> {
    let best = line.best.as_ref()?;
    if line.note.is_some() {
        return None;
    }
    let normal = line.normal.as_ref()?;
    let (long, short, leg) = line.legs(best.direction)?;
    let net = leg.net_to_normal_pct?;
    // 下单路径要求可成交价差为正、收敛到 0 也划算（价差单的硬性条件）：不满足的信号去了也会被拒。
    if net < settings.min_net_pct
        || leg.quote.entry_pct <= Decimal::ZERO
        || leg.net_to_zero_pct <= Decimal::ZERO
    {
        return None;
    }
    // 预估利润已经扣过往返手续费。开启核算后，这段时间若还要付资金费，把支付额再加进去：
    // 支付额 + 手续费 ≥ 预估利润就不合格。净收取不算成负成本，费率对不上也不猜。
    if let Some(hours) = settings.funding_check_hours {
        let profit_pct = net;
        let fee_pct = line.fee_round_trip_pct?;
        let funding_cost_pct = funding_carry_pct(report?, &line.base, long, short, hours)?
            .min(Decimal::ZERO)
            .abs();
        if funding_cost_pct + fee_pct >= profit_pct {
            return None;
        }
    }
    let target_pct = if settings.back_to_normal {
        Some(target_for(best.direction, normal.median)?)
    } else {
        None
    };
    Some(Candidate {
        base: line.base.clone(),
        long,
        short,
        target_pct,
        entry_pct: leg.quote.entry_pct,
        net_to_normal_pct: net,
        signal_sec: 0,
    })
}

/// 开仓后指定小时的资金费（%）。空腿收、多腿付；任一边对不上当前费率就返回空。
fn funding_carry_pct(
    report: &arb_scanner::ScanReport,
    base: &str,
    long: Venue,
    short: Venue,
    hours: u32,
) -> Option<Decimal> {
    let long_rate = current_rate(report, base, long)?;
    let short_rate = current_rate(report, base, short)?;
    if long_rate.interval_h == 0 || short_rate.interval_h == 0 {
        return None;
    }
    let long_cost =
        long_rate.period_rate * Decimal::from(hours) / Decimal::from(long_rate.interval_h);
    let short_cost =
        short_rate.period_rate * Decimal::from(hours) / Decimal::from(short_rate.interval_h);
    Some(((short_cost - long_cost) * Decimal::ONE_HUNDRED).round_dp(5))
}

fn current_rate<'a>(
    report: &'a arb_scanner::ScanReport,
    base: &str,
    venue: Venue,
) -> Option<&'a arb_core::MarketSnapshot> {
    report.symbols.iter().find_map(|view| {
        view.symbol
            .base
            .eq_ignore_ascii_case(base)
            .then(|| {
                view.rates
                    .iter()
                    .find(|rate| rate.venue == venue && rate.symbol.base.eq_ignore_ascii_case(base))
            })
            .flatten()
    })
}

/// 自动交易关着时，只记监控自己的信号。不读面板门槛，也不开始开仓计时。
fn record_monitor_signals(view: &View) {
    let path = crate::opportunity_log::path_from_env();
    let Some(path) = path.as_deref() else {
        return;
    };
    let at = Utc::now();
    for line in &view.lines {
        crate::opportunity_log::record_monitor_signal(Some(path), at, line);
    }
}

/// 把这一轮已经达到自动入场条件的行记到旁路文件。写失败不影响这一轮检查。
fn record_auto_lines(
    view: &View,
    settings: &Settings,
    report: Option<&arb_scanner::ScanReport>,
    since: &HashMap<HoldKey, Instant>,
    mode: Mode,
) {
    let path = crate::opportunity_log::path_from_env();
    let Some(path) = path.as_deref() else {
        return;
    };
    let now = Instant::now();
    let at = Utc::now();
    for line in &view.lines {
        let Some(candidate) = candidate_of_with(line, settings, report) else {
            continue;
        };
        let held = since.get(&candidate.hold_key()).map_or(0, |started| {
            now.saturating_duration_since(*started).as_secs()
        });
        crate::opportunity_log::record_auto(
            Some(path),
            at,
            line,
            settings,
            report,
            held,
            Some(mode),
        );
    }
}

/// 下单结果怎么处理。
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// 两腿都建好了。
    Opened(String),
    /// 开仓以回滚收场（计入连续失败）。
    Unwound(String),
    /// 闸门拒绝（深度、净收益、规则、限额）：这个合约冷却。
    Rejected,
    /// 交易台忙：稍后再试。
    Busy,
    /// 暂停开新仓 / 实盘没连上：等。
    Paused,
    /// 执行中断、结果未知，或回滚没完成仍有敞口：立刻关闭自动交易。
    Interrupted(String),
    /// 配置类错误（请求不合法、只读模式）：关闭自动交易。
    Misconfigured,
}

/// 按开仓接口的状态码与返回体分类。
pub fn classify(status: StatusCode, body: &Value) -> Outcome {
    if status.is_success() {
        let id = body["position"]["id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        return match body["position"]["status"].as_str() {
            Some("open") => Outcome::Opened(id),
            // 只有回滚完成、仓位已归零才算连续失败。`unwinding` 还有敞口，
            // 不能当成已经回滚完；缺状态或别的终态也不能假装没事。
            Some("unwound") => Outcome::Unwound(id),
            Some(status) => {
                Outcome::Interrupted(format!("仓位 {id} 状态是 {status}，不是已开仓或已回滚完成"))
            }
            None => Outcome::Interrupted(format!("仓位 {id} 缺少状态，不能确认是否还有敞口")),
        };
    }
    match status {
        StatusCode::CONFLICT
            if body["error"]
                .as_str()
                .is_some_and(|e| e.contains("正在进行")) =>
        {
            Outcome::Busy
        }
        StatusCode::LOCKED | StatusCode::SERVICE_UNAVAILABLE => Outcome::Paused,
        StatusCode::UNPROCESSABLE_ENTITY | StatusCode::CONFLICT => Outcome::Rejected,
        StatusCode::INTERNAL_SERVER_ERROR => Outcome::Interrupted(String::new()),
        _ => Outcome::Misconfigured,
    }
}

/// 自动交易器。
pub struct AutoTrader {
    path: Option<PathBuf>,
    inner: Mutex<Inner>,
    /// 两笔自动开仓之间的最短间隔（`ARB_RH_AUTO_OPEN_GAP_SEC`）。
    open_gap: Duration,
}

struct Inner {
    persisted: Persisted,
    events: VecDeque<Event>,
    cooldown: HashMap<String, Instant>,
    retry_at: Option<Instant>,
    last_open: Option<Instant>,
    /// 达到门槛的（合约, 多腿）从什么时候开始连续成立。
    since: HashMap<HoldKey, Instant>,
    /// 这一刻为什么没下单。
    status: String,
    attempting: Option<String>,
}

impl AutoTrader {
    pub fn load(dir: &std::path::Path) -> Arc<Self> {
        let path = dir.join("auto.json");
        let persisted = match std::fs::read_to_string(&path) {
            Ok(raw) => serde_json::from_str::<Persisted>(&raw).unwrap_or_else(|error| {
                // 读不懂就当作关闭：状态不明时不自动下单。
                warn!(path = %path.display(), "自动交易设置读不懂，按关闭处理：{error}");
                Persisted {
                    disabled_reason: Some(format!("设置文件读不懂（{error}），已按关闭处理")),
                    ..Persisted::default()
                }
            }),
            Err(_) => Persisted::default(),
        };
        let open_gap = open_gap_from_env();
        info!(
            open_gap_sec = open_gap.as_secs(),
            "价差自动开仓间隔（ARB_RH_AUTO_OPEN_GAP_SEC）"
        );
        Arc::new(Self {
            path: Some(path),
            open_gap,
            inner: Mutex::new(Inner {
                persisted,
                events: VecDeque::new(),
                cooldown: HashMap::new(),
                since: HashMap::new(),
                retry_at: None,
                last_open: None,
                status: "未开启".into(),
                attempting: None,
            }),
        })
    }

    #[cfg(test)]
    pub fn in_memory(settings: Settings) -> Arc<Self> {
        Arc::new(Self {
            path: None,
            open_gap: MIN_OPEN_GAP,
            inner: Mutex::new(Inner {
                persisted: Persisted {
                    settings,
                    ..Persisted::default()
                },
                events: VecDeque::new(),
                cooldown: HashMap::new(),
                since: HashMap::new(),
                retry_at: None,
                last_open: None,
                status: String::new(),
                attempting: None,
            }),
        })
    }

    pub async fn settings(&self) -> Settings {
        self.inner.lock().await.persisted.settings.clone()
    }

    /// 自动交易被关掉时记下的原因。正常关闭是 `None`。
    pub async fn disabled_reason(&self) -> Option<String> {
        self.inner.lock().await.persisted.disabled_reason.clone()
    }

    fn save(&self, persisted: &Persisted) {
        let Some(path) = &self.path else { return };
        let result = serde_json::to_string_pretty(persisted)
            .map_err(std::io::Error::other)
            .and_then(|body| {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let tmp = path.with_extension("json.tmp");
                let mut options = std::fs::OpenOptions::new();
                options.write(true).create(true).truncate(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                let mut file = options.open(&tmp)?;
                std::io::Write::write_all(&mut file, body.as_bytes())?;
                file.sync_all()?;
                std::fs::rename(&tmp, path)
            });
        if let Err(error) = result {
            error!(path = %path.display(), "自动交易设置写不进文件：{error}");
        }
    }

    fn push(inner: &mut Inner, kind: &'static str, symbol: Option<&str>, text: String) {
        inner.events.push_front(Event {
            at: Utc::now(),
            kind,
            symbol: symbol.map(str::to_string),
            text,
        });
        inner.events.truncate(EVENTS);
    }

    /// 关闭自动交易并记下原因。
    async fn disable(&self, reason: String) {
        let mut inner = self.inner.lock().await;
        inner.persisted.settings.enabled = false;
        inner.persisted.disabled_reason = Some(reason.clone());
        Self::push(&mut inner, "disabled", None, reason);
        self.save(&inner.persisted);
    }

    /// 新设置。返回保存后的设置；启用实盘需要 `confirm_live`。
    pub async fn update(
        &self,
        settings: Settings,
        max_size: Option<Decimal>,
        confirm_live: bool,
    ) -> Result<Settings, String> {
        settings.validate(max_size)?;
        let mut inner = self.inner.lock().await;
        let was = inner.persisted.settings.clone();
        let turning_on_live = settings.enabled
            && settings.mode == Mode::Live
            && !(was.enabled && was.mode == Mode::Live);
        if turning_on_live && !confirm_live {
            return Err("开启实盘自动交易需要确认：confirm 必须是 LIVE".into());
        }
        if settings.enabled && !was.enabled {
            // 重新开启：连续失败清零，冷却与重试时刻清空。
            inner.persisted.failures = 0;
            inner.persisted.disabled_reason = None;
            inner.cooldown.clear();
            inner.retry_at = None;
        }
        let text = describe(&settings);
        inner.persisted.settings = settings.clone();
        Self::push(&mut inner, "settings", None, text);
        self.save(&inner.persisted);
        Ok(settings)
    }

    /// 给页面的状态。
    pub async fn snapshot(&self, exposed: Option<&[(String, String)]>) -> Value {
        let inner = self.inner.lock().await;
        let today = Utc::now().date_naive();
        let mode = inner.persisted.settings.mode;
        let opened_today = inner
            .persisted
            .opened
            .iter()
            .filter(|o| o.mode == mode && o.at.date_naive() == today)
            .count();
        let open_auto: Vec<&Opened> = exposed.map_or_else(Vec::new, |exposed| {
            inner
                .persisted
                .opened
                .iter()
                .filter(|o| o.mode == mode && exposed.iter().any(|(id, _)| id == &o.id))
                .collect()
        });
        let now = Instant::now();
        let cooldown: Vec<Value> = inner
            .cooldown
            .iter()
            .filter(|(_, until)| **until > now)
            .map(|(base, until)| json!({ "symbol": base, "sec": until.duration_since(now).as_secs() }))
            .collect();
        json!({
            "settings": inner.persisted.settings,
            "status": inner.status,
            "attempting": inner.attempting,
            "failures": inner.persisted.failures,
            "max_failures": MAX_FAILURES,
            "disabled_reason": inner.persisted.disabled_reason,
            "opened_today": opened_today,
            "open_positions": open_auto,
            "cooldown": cooldown,
            "events": inner.events,
        })
    }

    pub fn spawn(self: &Arc<Self>, state: Arc<AppState>) {
        let auto = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                if crate::shutdown::is_draining() {
                    return;
                }
                auto.step(&state).await;
            }
        });
    }

    /// 一次检查：够条件就下一单。
    async fn step(&self, state: &Arc<AppState>) {
        let settings = {
            let mut inner = self.inner.lock().await;
            if !inner.persisted.settings.enabled {
                inner.status = match &inner.persisted.disabled_reason {
                    Some(reason) => format!("已关闭：{reason}"),
                    None => "未开启".into(),
                };
                drop(inner);
                // 关着时只排队记录，不在这条每秒检查里写文件。
                let view = state.rh_spread.view().await;
                tokio::spawn(async move { record_monitor_signals(&view) });
                return;
            }
            if inner.retry_at.is_some_and(|at| at > Instant::now()) {
                return;
            }
            inner.persisted.settings.clone()
        };
        let live = settings.mode == Mode::Live;
        if live && !state.trade.live_can_trade() {
            self.set_status(if state.trade.live_opt().is_some() {
                "实盘是只读模式（ARB_WEB_LIVE=readonly），不能自动下单"
            } else {
                "实盘账户没连上，等重连"
            })
            .await;
            return;
        }
        if live && state.trade.opens_paused() {
            self.set_status("开新仓已暂停（Telegram /pause），发 /resume 恢复")
                .await;
            return;
        }
        let view = state.rh_spread.view().await;
        let report = state
            .cache
            .get()
            .await
            .map(|snapshot| snapshot.report.clone());
        let now = Utc::now();
        // 先只更新计时：没有任何一条达到门槛时不必读台账（每秒读一次台账是白费）。
        let held_for_log = {
            let mut inner = self.inner.lock().await;
            track_holds_with(
                &view,
                &settings,
                report.as_ref(),
                &mut inner.since,
                Instant::now(),
            );
            inner.since.clone()
        };
        let log_view = view.clone();
        let log_settings = settings.clone();
        let log_report = report.clone();
        let mode = settings.mode;
        tokio::spawn(async move {
            record_auto_lines(
                &log_view,
                &log_settings,
                log_report.as_ref(),
                &held_for_log,
                mode,
            );
        });
        {
            let mut inner = self.inner.lock().await;
            if inner.since.is_empty() {
                inner.status = format!(
                    "监控中：没有回到正常净收益 ≥ {}%{}的信号",
                    settings.min_net_pct.normalize(),
                    settings.funding_check_hours.map_or(String::new(), |hours| {
                        format!("、且开仓后 {hours} 小时资金费加手续费仍小于预估利润")
                    })
                );
                return;
            }
        }
        let exposed = match state.trade.exposed_positions(live).await {
            Ok(exposed) => exposed,
            Err(error) => {
                self.set_status(&format!("读不了台账：{error}")).await;
                return;
            }
        };
        let candidate = {
            let mut inner = self.inner.lock().await;
            let today = now.date_naive();
            let mode = settings.mode;
            let opened_today = inner
                .persisted
                .opened
                .iter()
                .filter(|o| o.mode == mode && o.at.date_naive() == today)
                .count();
            let open_auto = inner
                .persisted
                .opened
                .iter()
                .filter(|o| o.mode == mode && exposed.iter().any(|(id, _)| id == &o.id))
                .count();
            // 计时每秒都更新（哪怕这一刻因为限额不下单），免得限额解除时拿旧的起点当「已保持很久」。
            track_holds_with(
                &view,
                &settings,
                report.as_ref(),
                &mut inner.since,
                Instant::now(),
            );
            if open_auto >= settings.max_positions {
                inner.status = format!(
                    "自动仓位已到上限（{open_auto}/{}），等平仓",
                    settings.max_positions
                );
                return;
            }
            if opened_today >= settings.daily_max_opens {
                inner.status = format!("今天（UTC）已自动开 {opened_today} 笔，到上限了");
                return;
            }
            if inner
                .last_open
                .is_some_and(|at| at.elapsed() < self.open_gap)
            {
                inner.status = format!("刚开过一笔，间隔 {} 秒再看", self.open_gap.as_secs());
                return;
            }
            let instant = Instant::now();
            track_holds_with(&view, &settings, report.as_ref(), &mut inner.since, instant);
            let since = inner.since.clone();
            let held = |c: &Candidate| {
                since
                    .get(&c.hold_key())
                    .map_or(0, |at| instant.saturating_duration_since(*at).as_secs())
            };
            let cooldown = inner.cooldown.clone();
            let busy = |base: &str| {
                if exposed
                    .iter()
                    .any(|(_, symbol)| symbol.base.eq_ignore_ascii_case(base))
                {
                    return Some("已有这个合约的仓位".to_string());
                }
                cooldown
                    .get(base)
                    .filter(|until| **until > instant)
                    .map(|until| {
                        format!(
                            "冷却中（还有 {} 秒）",
                            until.duration_since(instant).as_secs()
                        )
                    })
            };
            let live_venues: Option<Vec<Venue>> = state.trade.live_venues().map(<[Venue]>::to_vec);
            let tradable = |venue: Venue| match &live_venues {
                _ if !live => true,
                Some(venues) => venues.contains(&venue),
                None => false,
            };
            match pick_with(
                &view,
                &settings,
                report.as_ref(),
                now,
                &tradable,
                &held,
                &busy,
            ) {
                Ok(candidate) => {
                    inner.attempting = Some(candidate.base.clone());
                    inner.status = format!("正在为 {} 下单…", candidate.base);
                    candidate
                }
                Err(why) => {
                    inner.status = format!("监控中：{why}");
                    return;
                }
            }
        };
        self.attempt(state, &settings, &candidate).await;
    }

    async fn set_status(&self, status: &str) {
        self.inner.lock().await.status = status.to_string();
    }

    pub async fn auto_is_opening(&self) -> bool {
        self.inner.lock().await.attempting.is_some()
    }

    /// 手动下单前让出一次锁，给正在这一秒检查的自动交易先标记「正在下单」。
    pub async fn prefer_auto(&self) {
        let _ = self.inner.lock().await;
    }

    /// 手动信号下单。检查和下单标准与自动开仓相同，但不碰自动交易的状态。
    pub async fn open_manual(
        &self,
        state: &Arc<AppState>,
        settings: &Settings,
        candidate: &Candidate,
    ) -> Result<String, String> {
        let rules = arb_exec::TaskRules {
            basis_exit_pct: candidate.target_pct,
            take_profit_usdt: settings.take_profit_usdt,
            liq_protection_pct: settings.liq_protection_pct,
            auto_margin_pct: settings.auto_margin_pct,
            auto_margin_max_usdt: settings.auto_margin_max_usdt,
            ..arb_exec::TaskRules::default()
        };
        let symbol = format!("{}/USDT", candidate.base);
        let body = crate::trade::manual_open_body(
            settings.mode,
            &symbol,
            candidate.long,
            candidate.short,
            settings.size_usdt,
            settings.leverage,
            arb_exec::MarginMode::Isolated,
            &rules,
        );
        let label = format!(
            "{} 多 {} / 空 {}",
            candidate.base,
            candidate.long.as_str(),
            candidate.short.as_str()
        );
        let state = Arc::clone(state);
        let notified = label.clone();
        let live = settings.mode == Mode::Live;
        tokio::spawn(async move {
            let label = notified;
            for _ in 0..40 {
                if !state.rh_auto.auto_is_opening().await
                    && !crate::trade::desk_busy(&state.trade, live)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            if state.rh_auto.auto_is_opening().await || crate::trade::desk_busy(&state.trade, live)
            {
                state.alerts.notify_always(format!(
                    "手动开仓让给自动交易：{label}。自动开仓或平仓还没结束，这次没有下单。"
                ));
                return;
            }
            let _yield = state.rh_auto.prefer_auto().await;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if state.rh_auto.auto_is_opening().await || crate::trade::desk_busy(&state.trade, live)
            {
                state.alerts.notify_always(format!(
                    "手动开仓让给自动交易：{label}。自动交易刚开始下单，这次没有下单。"
                ));
                return;
            }
            let (status, response) =
                crate::trade::open_for_auto(Arc::clone(&state), body, true).await;
            if status.is_success() {
                state.alerts.notify_always(format!(
                    "手动开仓已提交 {label}。这笔不进入自动交易的计数、冷却和状态。"
                ));
            } else {
                state.alerts.notify_always(format!(
                    "手动开仓没通过：{label}。{}",
                    response["error"].as_str().unwrap_or("开仓检查没通过")
                ));
            }
        });
        Ok(format!(
            "已提交 {label}。结果另行通知，这笔不进入自动交易的计数、冷却和状态。"
        ))
    }

    async fn attempt(&self, state: &Arc<AppState>, settings: &Settings, candidate: &Candidate) {
        let rules = arb_exec::TaskRules {
            basis_exit_pct: candidate.target_pct,
            take_profit_usdt: settings.take_profit_usdt,
            liq_protection_pct: settings.liq_protection_pct,
            auto_margin_pct: settings.auto_margin_pct,
            auto_margin_max_usdt: settings.auto_margin_max_usdt,
            ..arb_exec::TaskRules::default()
        };
        let symbol = format!("{}/USDT", candidate.base);
        let body = crate::trade::auto_open_body(
            settings.mode,
            &symbol,
            candidate.long,
            candidate.short,
            settings.size_usdt,
            settings.leverage,
            arb_exec::MarginMode::Isolated,
            &rules,
        );
        let mode = if settings.mode == Mode::Live {
            "实盘"
        } else {
            "纸面"
        };
        let what = format!(
            "{mode} {} 多 {} / 空 {}，{} USDT × {}x，信号 {}s，回到正常预估 {}%{}",
            candidate.base,
            candidate.long,
            candidate.short,
            settings.size_usdt.normalize(),
            settings.leverage.normalize(),
            candidate.signal_sec,
            candidate.net_to_normal_pct.round_dp(3),
            candidate
                .target_pct
                .map(|t| format!("，收敛目标 {t}%"))
                .unwrap_or_default()
        );
        warn!(symbol = %candidate.base, "价差自动交易：尝试开仓 {what}");
        let (status, response) = crate::trade::open_for_auto(Arc::clone(state), body, true).await;
        let outcome = classify(status, &response);
        let reason = response["error"].as_str().unwrap_or_default().to_string();
        let now = Instant::now();
        let mut inner = self.inner.lock().await;
        inner.attempting = None;
        let base = Some(candidate.base.as_str());
        match outcome {
            Outcome::Opened(id) => {
                inner.persisted.failures = 0;
                inner.persisted.opened.push(Opened {
                    id: id.clone(),
                    symbol: symbol.clone(),
                    mode: settings.mode,
                    at: Utc::now(),
                });
                let excess = inner.persisted.opened.len().saturating_sub(OPENED_KEEP);
                inner.persisted.opened.drain(..excess);
                inner.last_open = Some(now);
                inner
                    .cooldown
                    .insert(candidate.base.clone(), now + SYMBOL_COOLDOWN);
                Self::push(&mut inner, "opened", base, format!("已开仓 {id}：{what}"));
                inner.status = format!("已开仓 {id}");
                self.save(&inner.persisted);
                drop(inner);
                info!(position = %id, "价差自动交易：开仓成功");
                if settings.mode == Mode::Paper {
                    state
                        .alerts
                        .notify_always(format!("🤖 自动交易（纸面）开仓 {id}：{what}"));
                } else {
                    // 实盘开仓的详细通知由下单路径发出；这里补一句是自动开的。
                    state
                        .alerts
                        .notify_always(format!("🤖 上面这笔实盘开仓 {id} 是价差自动交易开的。"));
                }
            }
            Outcome::Unwound(id) => {
                inner.persisted.failures += 1;
                let failures = inner.persisted.failures;
                inner.last_open = Some(now);
                inner
                    .cooldown
                    .insert(candidate.base.clone(), now + SYMBOL_COOLDOWN * 3);
                Self::push(
                    &mut inner,
                    "unwound",
                    base,
                    format!("开仓以回滚收场 {id}（连续 {failures} 次）：{what}"),
                );
                self.save(&inner.persisted);
                drop(inner);
                if failures >= MAX_FAILURES {
                    let reason = format!("开仓连续 {failures} 次以回滚收场");
                    self.disable(reason.clone()).await;
                    state.alerts.notify_always(format!(
                        "⛔ 价差自动交易已关闭：{reason}。查明原因后在面板上重新开启。"
                    ));
                }
            }
            Outcome::Rejected => {
                inner
                    .cooldown
                    .insert(candidate.base.clone(), now + SYMBOL_COOLDOWN);
                // 每次尝试都要对账、现扫一轮实盘场所：被拒后也歇一会儿，不连着打交易所。
                inner.retry_at = Some(now + REJECT_GAP);
                Self::push(
                    &mut inner,
                    "rejected",
                    base,
                    format!("下单前检查没通过（冷却 10 分钟）：{reason}"),
                );
                inner.status = format!("{} 没通过下单前检查", candidate.base);
            }
            Outcome::Busy => {
                inner.retry_at = Some(now + BUSY_RETRY);
                inner.status = "交易台忙（规则轮或手动操作），5 秒后再试".into();
            }
            Outcome::Paused => {
                inner.retry_at = Some(now + self.open_gap);
                Self::push(&mut inner, "paused", base, format!("没下单：{reason}"));
                inner.status = format!("没下单：{reason}");
            }
            Outcome::Interrupted(detail) => {
                drop(inner);
                let why = if detail.is_empty() { reason } else { detail };
                let text = format!("执行中断、结果未知：{why}（{what}）");
                error!(symbol = %candidate.base, "价差自动交易：{text}");
                self.disable(text.clone()).await;
                state.alerts.notify_always(format!(
                    "⛔ 价差自动交易已关闭：{text}。以台账与对账为准，去持仓页核对后再决定是否重新开启。"
                ));
            }
            Outcome::Misconfigured => {
                drop(inner);
                let text = format!("下单请求被拒（{}）：{reason}", status.as_u16());
                self.disable(text.clone()).await;
                state
                    .alerts
                    .notify_always(format!("⛔ 价差自动交易已关闭：{text}"));
            }
        }
    }
}

/// 设置的一句话描述（事件与通知用）。
pub fn describe(settings: &Settings) -> String {
    let mut parts = vec![format!(
        "{} {}，{} USDT × {}x，门槛 ≥ {}%，保持 {} 秒",
        if settings.enabled { "开启" } else { "关闭" },
        if settings.mode == Mode::Live {
            "实盘"
        } else {
            "纸面"
        },
        settings.size_usdt.normalize(),
        settings.leverage.normalize(),
        settings.min_net_pct.normalize(),
        settings.hold_sec
    )];
    if settings.back_to_normal {
        parts.push("回到正常基差平仓".into());
    }
    if let Some(tp) = settings.take_profit_usdt {
        parts.push(format!("止盈 {} USDT", tp.normalize()));
    }
    if let Some(pct) = settings.liq_protection_pct {
        parts.push(format!("爆仓保护 {}%", pct.normalize()));
    }
    if let (Some(pct), Some(max)) = (settings.auto_margin_pct, settings.auto_margin_max_usdt) {
        parts.push(format!(
            "自动追加保证金 {}% / 上限 {} USDT",
            pct.normalize(),
            max.normalize()
        ));
    }
    parts.push(format!(
        "同时最多 {} 笔、每日最多 {} 笔",
        settings.max_positions, settings.daily_max_opens
    ));
    if !settings.symbols.is_empty() {
        parts.push(format!("只做 {}", settings.symbols.join(",")));
    }
    parts.push(match settings.funding_check_hours {
        Some(hours) => format!("开仓后 {hours} 小时资金费核算"),
        None => "资金费核算关闭".into(),
    });
    parts.join("；")
}

// ───────────────────────────── 接口 ─────────────────────────────

/// `GET /api/rh-spread/auto`：设置、状态、最近事件。要令牌（含仓位 id 与下单结果）。
pub async fn api_get(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(denied) = state.trade.authorize(&headers) {
        return denied.into_response();
    }
    let settings = state.rh_auto.settings().await;
    let exposed = state
        .trade
        .exposed_positions(settings.mode == Mode::Live)
        .await
        .ok()
        .map(|list| {
            list.into_iter()
                .map(|(id, symbol)| (id, symbol.to_string()))
                .collect::<Vec<_>>()
        });
    let mut body = state.rh_auto.snapshot(exposed.as_deref()).await;
    body["max_position_usdt"] = json!(state.settings.max_position_usdt);
    body["live_can_trade"] = json!(state.trade.live_can_trade());
    body["paper_watch_sec"] = json!(state.trade.paper_watch_sec());
    body["monitor_size_usdt"] = json!(state.rh_spread.view().await.size_usdt);
    Json(body).into_response()
}

#[derive(Debug, Deserialize)]
pub struct UpdateBody {
    #[serde(flatten)]
    settings: Settings,
    /// 开启实盘自动交易时必须是 `LIVE`。
    confirm: Option<String>,
}

/// `POST /api/rh-spread/auto`：保存设置（整套替换）。
pub async fn api_set(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<UpdateBody>,
) -> Response {
    if let Err(denied) = state.trade.authorize(&headers) {
        return denied.into_response();
    }
    let mut settings = body.settings;
    settings.symbols = settings
        .symbols
        .iter()
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    if settings.enabled && settings.mode == Mode::Live && !state.trade.live_can_trade() {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": "实盘没连上或是只读模式，不能开启实盘自动交易" })),
        )
            .into_response();
    }
    let confirm = body.confirm.as_deref().is_some_and(|c| c.trim() == "LIVE");
    let max = state
        .settings
        .max_position_usdt
        .and_then(Decimal::from_f64_retain);
    match state.rh_auto.update(settings, max, confirm).await {
        Ok(saved) => {
            crate::opportunity_log::remember_auto_settings(&saved);
            warn!("价差自动交易设置已更新：{}", describe(&saved));
            state
                .alerts
                .notify_always(format!("🤖 价差自动交易设置已更新：{}", describe(&saved)));
            Json(json!({ "settings": saved })).into_response()
        }
        Err(message) => {
            (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rh_spread::{Best, Connected, DirectionQuote, Leg, Normal, Session};

    fn dec(raw: &str) -> Decimal {
        raw.parse().unwrap()
    }

    fn leg(entry: &str, zero: &str, normal: &str) -> Leg {
        Leg {
            quote: DirectionQuote {
                entry_pct: dec(entry),
                exit_cross_pct: dec("0.02"),
            },
            net_to_zero_pct: dec(zero),
            net_to_normal_pct: Some(dec(normal)),
        }
    }

    fn line(base: &str, direction: &'static str, leg: Leg, median: f64) -> Line {
        let (long_a, long_b) = if direction == "long_a" {
            (Some(leg), None)
        } else {
            (None, Some(leg))
        };
        Line {
            pair: crate::rh_spread::pairs::Pair::RH.id(),
            a: Venue::Arcus,
            b: Venue::LighterRh,
            fee_round_trip_pct: Some(dec("0.045")),
            base: base.into(),
            category: "EQUITIES".into(),
            session: Session::Rth,
            basis_pct: Some(dec("-0.3")),
            normal: Some(Normal {
                session: Session::Rth,
                median,
                p10: median - 0.03,
                p90: median + 0.03,
                mad: 0.01,
                minutes: 900,
            }),
            normal_missing_minutes: 0,
            z: Some(-10.0),
            long_a,
            long_b,
            best: Some(Best {
                direction,
                signal: false,
                signal_sec: 0,
                net_usdt: None,
            }),
            age_sec: Some(0),
            note: None,
        }
    }

    fn view(lines: Vec<Line>) -> View {
        View {
            enabled: true,
            size_usdt: Decimal::from(2000),
            alert_net_pct: dec("0.05"),
            min_minutes: 120,
            window_days: 7,
            pairs: Vec::new(),
            connected: Connected {
                venues: [(Venue::Arcus, true), (Venue::LighterRh, true)].into(),
                reconnects: 0,
            },
            updated_at: Some(Utc::now()),
            lines,
            error: None,
        }
    }

    fn settings() -> Settings {
        Settings {
            enabled: true,
            min_net_pct: dec("0.05"),
            ..Settings::default()
        }
    }

    #[test]
    fn a_qualifying_line_becomes_an_order_with_the_back_to_normal_target_in_position_terms() {
        // 页面基差 (Arcus − RH) 正常 −0.11%；多 Arcus / 空 RH 的持仓基差 (空 − 多) = −页面基差 → 目标 +0.11%。
        let nvda = line("NVDA", "long_a", leg("0.30", "0.20", "0.09"), -0.11);
        let c = candidate_of(&nvda, &settings()).expect("够门槛");
        assert_eq!((c.long, c.short), (Venue::Arcus, Venue::LighterRh));
        assert_eq!(c.target_pct, Some(dec("0.11")));
        let reverse = line("NVDA", "long_b", leg("0.30", "0.20", "0.09"), -0.11);
        let c = candidate_of(&reverse, &settings()).unwrap();
        assert_eq!((c.long, c.short), (Venue::LighterRh, Venue::Arcus));
        assert_eq!(c.target_pct, Some(dec("-0.11")));
        // 只开止盈、不按正常基差平：不带收敛目标。
        let no_target = Settings {
            back_to_normal: false,
            take_profit_usdt: Some(Decimal::ONE),
            ..settings()
        };
        assert_eq!(candidate_of(&nvda, &no_target).unwrap().target_pct, None);
    }

    #[test]
    fn lines_the_order_path_would_reject_are_never_picked() {
        let s = settings();
        // 其它组同样适用（规则相同）：多 HL-xyz / 空 RH。
        let mut other = line("NVDA", "long_a", leg("0.3", "0.2", "0.09"), -0.11);
        other.pair = "hyperliquid-xyz:lighter-rh".into();
        other.a = Venue::HyperliquidXyz;
        let c = candidate_of(&other, &s).expect("别的组也做");
        assert_eq!((c.long, c.short), (Venue::HyperliquidXyz, Venue::LighterRh));
        // 开仓价差为负（如 io ↔ RH 的 ANTHROPIC 长期差 2%）：哪个组都不做。
        let mut io = line("ANTHROPIC", "long_b", leg("-2.17", "-2.19", "0.087"), -2.27);
        io.pair = "hyperliquid-io:lighter-rh".into();
        io.a = Venue::HyperliquidIo;
        assert!(candidate_of(&io, &s).is_none());
        // 回到正常净收益不够门槛。
        assert!(candidate_of(&line("A", "long_a", leg("0.3", "0.2", "0.04"), -0.1), &s).is_none());
        // 可成交价差不为正（价差单的硬性条件）。
        assert!(
            candidate_of(&line("A", "long_a", leg("-0.01", "0.2", "0.09"), -0.1), &s).is_none()
        );
        // 收敛到 0 也不划算（下单路径按它拒绝）。
        assert!(
            candidate_of(&line("A", "long_a", leg("0.1", "-0.01", "0.09"), -0.1), &s).is_none()
        );
        // 盘口过期等。
        let mut stale = line("A", "long_a", leg("0.3", "0.2", "0.09"), -0.1);
        stale.note = Some("Arcus 盘口 20 秒没更新".into());
        assert!(candidate_of(&stale, &s).is_none());
        // 没有正常样本。
        let mut fresh = line("A", "long_a", leg("0.3", "0.2", "0.09"), -0.1);
        fresh.normal = None;
        assert!(candidate_of(&fresh, &s).is_none());
        // 目标超出规则允许的 ±5%：不截断成另一个意思的数，直接不做。
        assert!(candidate_of(&line("A", "long_a", leg("0.3", "0.2", "0.09"), 7.0), &s).is_none());
    }

    #[test]
    fn a_six_hour_funding_cost_that_covers_the_profit_is_not_a_candidate() {
        let row = line("NVDA", "long_a", leg("0.30", "0.20", "0.09"), -0.11);
        // long_a：多 Arcus、空 Lighter RH。多腿每小时付 0.01%，空腿不收：6 小时付 0.06%，
        // 加上往返手续费 0.045%，合计 0.105%，不低于 0.09% 的预估利润。
        let report = funding_report(
            "NVDA",
            Venue::Arcus,
            dec("0.0001"),
            1,
            Venue::LighterRh,
            Decimal::ZERO,
            1,
        );
        let funding = funding_carry_pct(&report, "NVDA", Venue::Arcus, Venue::LighterRh, 6);
        let mut checking = settings();
        checking.funding_check_hours = Some(6);
        assert!(
            candidate_of_with(&row, &checking, Some(&report)).is_none(),
            "6 小时要付的资金费加手续费盖过 0.09% 利润，funding={funding:?}"
        );
        assert!(
            candidate_of_with(&row, &settings(), Some(&report)).is_some(),
            "默认关闭资金费核算"
        );
        let cheaper = funding_report(
            "NVDA",
            Venue::Arcus,
            Decimal::ZERO,
            1,
            Venue::LighterRh,
            Decimal::ZERO,
            1,
        );
        assert!(candidate_of_with(&row, &checking, Some(&cheaper)).is_some());
        assert!(candidate_of_with(&row, &checking, None).is_none());
    }

    fn funding_report(
        base: &str,
        long: Venue,
        long_rate: Decimal,
        long_hours: u32,
        short: Venue,
        short_rate: Decimal,
        short_hours: u32,
    ) -> arb_scanner::ScanReport {
        let rate = |venue: Venue, period: Decimal, hours: u32| arb_core::MarketSnapshot {
            venue,
            symbol: arb_core::Symbol::perp(base, "USDT"),
            period_rate: period,
            interval_h: hours,
            interval_assumed: false,
            next_funding_at: Utc::now(),
            next_funding_estimated: false,
            taker_fee: Some(dec("0.0001")),
            mark_price: Some(Decimal::ONE),
            index_price: Some(Decimal::ONE),
            best_bid: None,
            best_ask: None,
            bid_size_usdt: None,
            ask_size_usdt: None,
            open_interest_usdt: None,
            quote_volume_24h: None,
            max_leverage: None,
            maintenance_margin: None,
            oi_capped: false,
        };
        arb_scanner::ScanReport {
            generated_at: Utc::now(),
            fee_per_side: dec("0.0005"),
            amortize_days: Decimal::ONE,
            spread_hold_days: Decimal::ONE,
            max_entry_basis_pct: None,
            min_venues: 2,
            venues: Vec::new(),
            symbols: vec![arb_scanner::SymbolView {
                symbol: arb_core::Symbol::perp(base, "USDT"),
                rates: vec![
                    rate(long, long_rate, long_hours),
                    rate(short, short_rate, short_hours),
                ],
                funding: Vec::new(),
                spread: Vec::new(),
            }],
            unverified: Vec::new(),
            suspicious: Vec::new(),
            gated: Vec::new(),
            totals: arb_scanner::Totals::default(),
        }
    }

    #[test]
    fn picking_waits_for_the_hold_time_and_skips_busy_symbols() {
        let s = Settings {
            hold_sec: 10,
            ..settings()
        };
        let v = view(vec![
            line("NVDA", "long_a", leg("0.3", "0.2", "0.09"), -0.11),
            line("SPY", "long_a", leg("0.3", "0.2", "0.08"), -0.11),
        ]);
        let now = Utc::now();
        let none = |_: &str| None;
        let all = |_: Venue| true;
        let err = pick(&v, &s, now, &all, &|_| 3, &none).unwrap_err();
        assert!(err.contains("3/10"), "{err}");
        let held = |c: &Candidate| if c.base == "NVDA" { 12 } else { 30 };
        assert_eq!(pick(&v, &s, now, &all, &held, &none).unwrap().base, "NVDA");
        // 已有 NVDA 仓位：跳到下一条。
        let busy = |b: &str| (b == "NVDA").then(|| "已有这个合约的仓位".to_string());
        assert_eq!(pick(&v, &s, now, &all, &held, &busy).unwrap().base, "SPY");
        // 名单只做 SPY。
        let only = Settings {
            symbols: vec!["SPY".into()],
            ..s.clone()
        };
        assert_eq!(
            pick(&v, &only, now, &all, &held, &none).unwrap().base,
            "SPY"
        );
        // 行情断了、快照旧了：不下单。
        let mut down = v.clone();
        down.connected.venues.insert(Venue::Arcus, false);
        assert!(pick(&down, &s, now, &all, &held, &none).is_err());
        let mut old = v.clone();
        old.updated_at = Some(now - chrono::Duration::seconds(30));
        assert!(pick(&old, &s, now, &all, &held, &none).is_err());
        // 实盘只连了 Arcus（RH 没连上 / 没配 API）：两腿不全，不做。
        let arcus_only = |v: Venue| v == Venue::Arcus;
        assert!(pick(&v, &s, now, &arcus_only, &held, &none).is_err());
    }

    #[test]
    fn hold_timers_reset_when_a_line_stops_qualifying() {
        let s = settings();
        let mut since = HashMap::new();
        let t0 = Instant::now();
        let good = view(vec![line(
            "NVDA",
            "long_a",
            leg("0.3", "0.2", "0.09"),
            -0.11,
        )]);
        track_holds(&good, &s, &mut since, t0);
        track_holds(&good, &s, &mut since, t0 + Duration::from_secs(5));
        let key = ("NVDA".to_string(), Venue::Arcus, Venue::LighterRh);
        assert_eq!(since[&key], t0);
        let gone = view(vec![line(
            "NVDA",
            "long_a",
            leg("0.3", "0.2", "0.01"),
            -0.11,
        )]);
        track_holds(&gone, &s, &mut since, t0 + Duration::from_secs(6));
        assert!(since.is_empty());
        track_holds(&good, &s, &mut since, t0 + Duration::from_secs(7));
        assert_eq!(since[&key], t0 + Duration::from_secs(7));
    }

    #[test]
    fn order_responses_are_classified_conservatively() {
        let ok = json!({ "position": { "id": "live-1", "status": "open" } });
        assert_eq!(
            classify(StatusCode::OK, &ok),
            Outcome::Opened("live-1".into())
        );
        let unwound = json!({ "position": { "id": "live-2", "status": "unwound" } });
        assert_eq!(
            classify(StatusCode::OK, &unwound),
            Outcome::Unwound("live-2".into())
        );
        let busy =
            json!({ "error": "这张交易台上有一笔操作（或一轮规则监控）正在进行，等它结束再试" });
        assert_eq!(classify(StatusCode::CONFLICT, &busy), Outcome::Busy);
        let dirty = json!({ "error": "对账不干净，拒绝开新仓" });
        assert_eq!(classify(StatusCode::CONFLICT, &dirty), Outcome::Rejected);
        assert_eq!(
            classify(StatusCode::UNPROCESSABLE_ENTITY, &json!({})),
            Outcome::Rejected
        );
        assert_eq!(classify(StatusCode::LOCKED, &json!({})), Outcome::Paused);
        assert_eq!(
            classify(StatusCode::SERVICE_UNAVAILABLE, &json!({})),
            Outcome::Paused
        );
        // 执行中断（可能留下一条腿）：结果未知，必须停下来。
        assert_eq!(
            classify(StatusCode::INTERNAL_SERVER_ERROR, &json!({})),
            Outcome::Interrupted(String::new())
        );
        let unwinding = json!({"position": {"id": "live-3", "status": "unwinding"}});
        assert_eq!(
            classify(StatusCode::OK, &unwinding),
            Outcome::Interrupted("仓位 live-3 状态是 unwinding，不是已开仓或已回滚完成".into())
        );
        let missing = json!({"position": {"id": "live-4"}});
        assert_eq!(
            classify(StatusCode::OK, &missing),
            Outcome::Interrupted("仓位 live-4 缺少状态，不能确认是否还有敞口".into())
        );
        assert_eq!(
            classify(StatusCode::FORBIDDEN, &json!({})),
            Outcome::Misconfigured
        );
        assert_eq!(
            classify(StatusCode::BAD_REQUEST, &json!({})),
            Outcome::Misconfigured
        );
    }

    #[test]
    fn settings_need_an_exit_rule_sane_limits_and_live_confirmation() {
        let max = Some(Decimal::from(1000));
        assert!(settings().validate(max).is_ok());
        let bad = |s: Settings| s.validate(max).unwrap_err();
        assert!(
            bad(Settings {
                size_usdt: Decimal::from(2000),
                ..settings()
            })
            .contains("ARB_MAX_POSITION_USDT")
        );
        assert!(
            bad(Settings {
                back_to_normal: false,
                take_profit_usdt: None,
                ..settings()
            })
            .contains("退出规则")
        );
        assert!(
            bad(Settings {
                leverage: dec("2.5"),
                ..settings()
            })
            .contains("整数")
        );
        assert!(
            bad(Settings {
                max_positions: 0,
                ..settings()
            })
            .contains("同时持仓")
        );
        assert!(
            bad(Settings {
                max_positions: 31,
                ..settings()
            })
            .contains("1 到 30")
        );
        assert!(
            Settings {
                max_positions: 30,
                ..settings()
            }
            .validate(max)
            .is_ok()
        );
        assert!(
            bad(Settings {
                symbols: vec!["NV DA".into()],
                ..settings()
            })
            .contains("字母数字")
        );
        assert!(
            bad(Settings {
                auto_margin_pct: Some(dec("15")),
                ..settings()
            })
            .contains("同时")
        );
        assert!(
            bad(Settings {
                funding_check_hours: Some(5),
                ..settings()
            })
            .contains("3 小时、6 小时")
        );
        assert!(
            Settings {
                funding_check_hours: Some(3),
                ..settings()
            }
            .validate(max)
            .is_ok()
        );
        assert!(
            Settings {
                auto_margin_pct: Some(dec("15")),
                auto_margin_max_usdt: Some(dec("100")),
                ..settings()
            }
            .validate(max)
            .is_ok()
        );

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let auto = AutoTrader::in_memory(Settings::default());
            let live = Settings {
                mode: Mode::Live,
                ..settings()
            };
            let err = auto.update(live.clone(), max, false).await.unwrap_err();
            assert!(err.contains("LIVE"));
            assert!(!auto.settings().await.enabled, "没确认就不开");
            auto.update(live.clone(), max, true).await.unwrap();
            assert!(auto.settings().await.enabled);
            // 已经开着实盘，改参数不必再确认。
            auto.update(
                Settings {
                    size_usdt: Decimal::from(800),
                    ..live
                },
                max,
                false,
            )
            .await
            .unwrap();
            auto.disable("测试".into()).await;
            let snapshot = auto.snapshot(Some(&[])).await;
            assert_eq!(snapshot["settings"]["enabled"], json!(false));
            assert_eq!(snapshot["disabled_reason"], json!("测试"));
        });
    }

    #[test]
    fn auto_orders_carry_the_rules_and_go_through_the_confirmed_spread_path() {
        let rules = arb_exec::TaskRules {
            basis_exit_pct: Some(dec("0.110")),
            take_profit_usdt: Some(dec("1.50")),
            ..arb_exec::TaskRules::default()
        };
        let body = crate::trade::auto_open_body(
            Mode::Live,
            "NVDA/USDT",
            Venue::Arcus,
            Venue::LighterRh,
            Decimal::from(500),
            Decimal::from(3),
            arb_exec::MarginMode::Isolated,
            &rules,
        );
        let text = format!("{body:?}");
        for part in [
            "Live",
            "NVDA/USDT",
            "\"arcus\"",
            "\"lighter-rh\"",
            "\"spread\"",
            "\"0.11\"",
            "\"1.5\"",
            "confirm: Some(\"NVDA\")",
            "daily_pnl: None",
        ] {
            assert!(text.contains(part), "{part} missing in {text}");
        }
    }
}
