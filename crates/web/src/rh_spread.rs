//! 跨场所同名合约价差监控（只读，不下单；自动交易见 `rh_auto`）。
//!
//! 最早只做 Lighter RH ↔ Arcus（都在 Robinhood Chain 上、都用 USDG 保证金）；现在按**组**监控，
//! 支持 Arcus、Lighter RH、Hyperliquid 主 dex 与 HIP-3 xyz / io 任意两家。
//! `ARB_RH_SPREAD_EXCLUDE` 只从这些组里拿掉场所，`ARB_RH_SPREAD_EXCLUDE_PAIRS` 只拿掉指定的两家配对；
//! 都不关实盘连接，也不平已有仓位。
//! 价格偶尔会偏离：一边比另一边贵出手续费加穿价以上。这里：
//!
//! 1. 用各家的行情 WebSocket 维护本地盘口（REST 轮询会打穿 Lighter RH 的 IP 限频，
//!    并挤占实盘下单用的额度）；每家只连一条，几组共用；
//! 2. 每秒按「这笔名义吃完深度的均价」算两个方向的可成交价差，扣掉两家各开平一次的吃单费
//!    和立即平仓的穿价；
//! 3. 每分钟落盘一行中间价基差（每组一个文件前缀），按合约、按时段（盘中 / 盘后 / 周末 / 加密币全天）
//!    统计「正常水平」。股票类合约在休市时常常有**系统性**偏差：现价差大不等于会收敛到 0；
//! 4. 偏离正常水平、且「回到正常水平」的预估净收益超过门槛时推送 Telegram（同一组同一合约同一方向 30 分钟一条）。
//!    实盘台账里已经有敞口的币不推：已经在仓里，不必再喊去开。
//!
//! 基差一律是 **(a − b) / 均值**，方向叫 `long_a` / `long_b`（最早那组 a = Arcus、b = Lighter RH，与旧历史同号）。

mod book;
mod catalog;
mod feed;
pub mod history;
pub mod pairs;
pub mod session;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::alert::Alerter;
use arb_core::Venue;
pub use book::{DirectionQuote, LocalBook, mid_basis_pct, quote};
pub use history::{Normal, Row};
use pairs::{Catalog, Pair, PairMarket};
pub use session::{Session, classify};

/// 监控配置（环境变量）。
#[derive(Debug, Clone)]
pub struct Config {
    pub enabled: bool,
    /// 估算用的单笔名义（USDT）。
    pub size_usdt: Decimal,
    /// 提醒门槛：回到正常水平的预估净收益（%）。
    pub alert_net_pct: Decimal,
    /// 只看股票类（股票、指数、商品），不看加密币。
    pub equities_only: bool,
    /// 排除 Lighter RH 与 Hyperliquid 分类名单里的 Pre-IPO 合约。
    pub exclude_preipo: bool,
    pub dir: PathBuf,
    /// 监控哪几组。默认按实盘场所两两组合，`ARB_RH_SPREAD_EXCLUDE` 再拿掉场所，
    /// `ARB_RH_SPREAD_EXCLUDE_PAIRS` 再拿掉指定配对。
    pub pairs: Vec<Pair>,
    /// Entropy（`hyperliquid-io`）自返佣（`ARB_ENTROPY_SELF_REBATE`，2 = 200%）。
    pub entropy_rebate: Decimal,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let decimal =
            |name: &str, default: &str, min: Decimal, max: Decimal| -> anyhow::Result<Decimal> {
                let raw = var(name).unwrap_or_else(|| default.to_string());
                let value = arb_core::parse_decimal(&raw)
                    .ok_or_else(|| anyhow::anyhow!("{name} 必须是十进制数，收到 {raw:?}"))?;
                anyhow::ensure!(
                    (min..=max).contains(&value),
                    "{name} 必须在 {min} 到 {max} 之间，收到 {value}"
                );
                Ok(value)
            };
        let enabled = !matches!(
            var("ARB_RH_SPREAD")
                .as_deref()
                .map(str::to_ascii_lowercase)
                .as_deref(),
            Some("off" | "0" | "false")
        );
        Ok(Self {
            enabled,
            size_usdt: decimal(
                "ARB_RH_SPREAD_SIZE",
                "2000",
                Decimal::from(100),
                Decimal::from(100_000),
            )?,
            alert_net_pct: decimal(
                "ARB_RH_SPREAD_ALERT_PCT",
                "0.05",
                Decimal::new(1, 3),
                Decimal::from(5),
            )?,
            equities_only: matches!(
                var("ARB_RH_SPREAD_EQUITIES_ONLY").as_deref(),
                Some("1" | "on" | "true")
            ),
            exclude_preipo: matches!(
                var("ARB_RH_SPREAD_EXCLUDE_PREIPO").as_deref(),
                Some("1" | "on" | "true")
            ),
            dir: var("ARB_RH_SPREAD_DIR").map_or_else(|| PathBuf::from("rh-spread"), PathBuf::from),
            pairs: monitored_pairs(&var)?,
            entropy_rebate: arb_venues::hyperliquid::entropy_self_rebate_from_env()
                .map_err(|error| anyhow::anyhow!("{error}"))?,
        })
    }
}

/// 监控组。先按实盘场所两两组合，再按 `ARB_RH_SPREAD_EXCLUDE` 拿掉场所，
/// 再按 `ARB_RH_SPREAD_EXCLUDE_PAIRS` 拿掉指定配对。拿掉之后没有组就报错，不要悄悄监一个空名单。
fn monitored_pairs(var: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<Vec<Pair>> {
    let mut pairs = match var("ARB_RH_SPREAD_PAIRS") {
        Some(raw) => Pair::parse_list(&raw)
            .map_err(|error| anyhow::anyhow!("ARB_RH_SPREAD_PAIRS：{error}"))?,
        None => default_pairs(),
    };
    if let Some(raw) = var("ARB_RH_SPREAD_EXCLUDE") {
        let excluded = pairs::parse_excluded(&raw)
            .map_err(|error| anyhow::anyhow!("ARB_RH_SPREAD_EXCLUDE：{error}"))?;
        pairs = pairs::without_venues(&pairs, &excluded);
        if pairs.is_empty() {
            anyhow::bail!("ARB_RH_SPREAD_EXCLUDE 排除之后没有可监控的组");
        }
    }
    if let Some(raw) = var("ARB_RH_SPREAD_EXCLUDE_PAIRS") {
        let excluded = pairs::parse_excluded_pairs(&raw)
            .map_err(|error| anyhow::anyhow!("ARB_RH_SPREAD_EXCLUDE_PAIRS：{error}"))?;
        pairs = pairs::without_pairs(&pairs, &excluded);
        if pairs.is_empty() {
            anyhow::bail!("ARB_RH_SPREAD_EXCLUDE_PAIRS 排除之后没有可监控的组");
        }
    }
    Ok(pairs)
}

/// 默认的组：按凭据识别出的实盘场所（`ARB_LIVE_VENUES` 或自动识别）里价差监控支持的，两两组合。
/// 识别不出两家时退回 [`pairs::DEFAULT_PAIRS`]。
fn default_pairs() -> Vec<Pair> {
    let detected = arb_exec::live_connect::live_venues(None)
        .map(|selection| pairs::all_pairs(&selection.venues))
        .unwrap_or_default();
    if detected.is_empty() {
        Pair::parse_list(pairs::DEFAULT_PAIRS).expect("默认组合法")
    } else {
        detected
    }
}

/// 页面上一行：一组里一个合约此刻的状况。
#[derive(Debug, Clone, Serialize)]
pub struct Line {
    /// 组标识（`arcus:lighter-rh`）。
    pub pair: String,
    /// 基差的被减数 / 减数：(a − b)。
    pub a: Venue,
    pub b: Venue,
    pub base: String,
    pub category: String,
    pub session: Session,
    /// 往返两腿手续费（%）。
    pub fee_round_trip_pct: Option<Decimal>,
    /// 中间价基差（%）：(a − b) / 均值。
    pub basis_pct: Option<Decimal>,
    pub normal: Option<Normal>,
    /// 正常水平还差多少分钟样本（够了为 0）。
    pub normal_missing_minutes: usize,
    /// 断档后还在重新攒这一时段。数字可以显示，不能当开仓目标。
    #[serde(default)]
    pub normal_blocked: bool,
    /// 偏离正常水平几个 MAD。
    pub z: Option<f64>,
    /// 两个方向的报价与净收益：多 a 空 b / 多 b 空 a。
    pub long_a: Option<Leg>,
    pub long_b: Option<Leg>,
    /// 更好的那个方向。
    pub best: Option<Best>,
    /// 两家盘口里较旧的那本多久没更新（秒）。
    pub age_sec: Option<u64>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Leg {
    #[serde(flatten)]
    pub quote: DirectionQuote,
    /// 收敛到 0 时的净收益（%）：可成交价差 − 平仓穿价 − 往返手续费。
    pub net_to_zero_pct: Decimal,
    /// 回到正常水平时的净收益（%）。没有正常水平时为 `None`。
    pub net_to_normal_pct: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Best {
    /// `long_a` / `long_b`。
    pub direction: &'static str,
    /// 达到提醒条件（回到正常水平净收益 ≥ 门槛、样本够、盘口新鲜）。
    pub signal: bool,
    /// 信号已经连续保持了多少秒（[`SIGNAL_HOLD`] 之后才推送）。
    pub signal_sec: u64,
    /// 按 [`Config::size_usdt`] 折算的回到正常水平净收益（USDT）。
    pub net_usdt: Option<Decimal>,
}

/// 给接口的整体快照。
#[derive(Debug, Clone, Serialize)]
pub struct View {
    pub enabled: bool,
    pub size_usdt: Decimal,
    pub alert_net_pct: Decimal,
    pub min_minutes: usize,
    pub window_days: i64,
    /// 是否已开启 Pre-IPO 排除。
    pub exclude_preipo: bool,
    /// 当前读到的 Pre-IPO 币名（两家去重）。名单始终显示，是否排除由开关决定。
    pub preipo_bases: Vec<String>,
    /// 各组的概况（顺序 = 配置顺序）。
    pub pairs: Vec<PairView>,
    /// 各家行情连接。
    pub connected: Connected,
    pub updated_at: Option<DateTime<Utc>>,
    pub lines: Vec<Line>,
    pub error: Option<String>,
}

/// 一组的概况。
#[derive(Debug, Clone, Serialize)]
pub struct PairView {
    pub id: String,
    pub a: Venue,
    pub b: Venue,
    /// 同名合约数（Hyperliquid 的组要等首轮扫描核过身份才有）。
    pub markets: usize,
    pub history_minutes: usize,
    /// 这组的往返手续费范围（%）：合约之间可能不同（growth mode）。
    pub fee_min_pct: Option<Decimal>,
    pub fee_max_pct: Option<Decimal>,
    /// 还没发现市场的原因（等扫描、某家市场列表取不到）。
    pub note: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Connected {
    /// 场所 → 行情 WebSocket 是否连着。
    pub venues: std::collections::BTreeMap<Venue, bool>,
    pub reconnects: u64,
}

impl Connected {
    pub fn up(&self, venue: Venue) -> bool {
        self.venues.get(&venue).copied().unwrap_or(false)
    }
}

/// 快照式盘口（Arcus、Hyperliquid：盘口没变也推）超过这么久没更新就不算数（WebSocket 静默断流时不会报错）。
/// Hyperliquid 实测每 ~5 秒推一次，留出三倍余量。
const STALE: Duration = Duration::from_secs(15);
/// 信号要连续保持这么久才推送：一秒钟的闪烁（一档挂单被吃掉又补上）不值得叫人。
pub const SIGNAL_HOLD: Duration = Duration::from_secs(10);
/// 价差提醒自己的限频：每分钟最多这么多条。告警通道全局每分钟 6 条、与交易告警共用，
/// 一波行情同时亮十几个合约时不能把规则平仓、裸敞口这类告警挤掉。
pub const ALERTS_PER_MINUTE: usize = 2;

/// 这家的盘口是不是「没变也推」的快照：是的话它的年龄就是数据新鲜度。
/// Lighter 只推变化：安静的市场几秒没增量是常态，新鲜度靠连接活着保证（断线即清空盘口）。
pub fn snapshot_feed(venue: Venue) -> bool {
    venue != Venue::LighterRh
}

/// 估算一组里一个合约此刻的状况。纯计算，单测覆盖。
#[allow(clippy::too_many_arguments)]
pub fn evaluate(
    pair: Pair,
    market: &PairMarket,
    a: Option<&LocalBook>,
    b: Option<&LocalBook>,
    normal: Option<Normal>,
    normal_minutes: usize,
    session: Session,
    config: &Config,
    now: Instant,
) -> Line {
    let fee = market.round_trip_pct();
    let mut line = Line {
        pair: pair.id(),
        a: pair.a,
        b: pair.b,
        base: market.base.clone(),
        category: market.category.clone(),
        session,
        fee_round_trip_pct: fee,
        basis_pct: None,
        normal_missing_minutes: history::MIN_MINUTES.saturating_sub(normal_minutes),
        normal_blocked: false,
        normal: normal.clone(),
        z: None,
        long_a: None,
        long_b: None,
        best: None,
        age_sec: None,
        note: None,
    };
    let (Some(book_a), Some(book_b)) = (a, b) else {
        line.note = Some("等待两家盘口".into());
        return line;
    };
    let mut oldest: Option<(Venue, Duration)> = None;
    for (venue, book) in [(pair.a, book_a), (pair.b, book_b)] {
        if snapshot_feed(venue) {
            let age = now.saturating_duration_since(book.updated);
            if oldest.is_none_or(|(_, o)| age > o) {
                oldest = Some((venue, age));
            }
        }
    }
    if let Some((venue, age)) = oldest {
        line.age_sec = Some(age.as_secs());
        if age > STALE {
            line.note = Some(format!("{venue} 盘口 {} 秒没更新，不计算", age.as_secs()));
            return line;
        }
    }
    let Some(basis) = mid_basis_pct(book_a, book_b) else {
        line.note = Some("至少一家盘口为空或交叉".into());
        return line;
    };
    line.basis_pct = Some(basis);
    let Some(fee) = fee else {
        line.note = Some("至少一家的吃单费率不知道，不估净收益".into());
        return line;
    };
    if let Some(n) = &normal
        && let Some(b) = basis.to_f64()
    {
        // MAD 太小（几乎不动的合约）时下限 0.005%，免得一点抖动就几十个 σ。
        line.z = Some(((b - n.median) / n.mad.max(0.005) * 1000.0).round() / 1000.0);
    }
    let normal_median = normal
        .as_ref()
        .and_then(|n| Decimal::from_f64_retain(n.median))
        .map(|d| d.round_dp(5));
    // 方向：多 a 空 b 赚的是「a 相对 b 变贵」（基差上升）；反方向赚基差下降。
    // 回到正常水平的净收益 = 可成交价差 − 平仓穿价 − 手续费 −/＋ 正常水平（正常基差本身是收不回来的那部分）。
    let leg = |quote: DirectionQuote, sign: Decimal| {
        let net_to_zero_pct = (quote.entry_pct - quote.exit_cross_pct - fee).round_dp(5);
        Leg {
            net_to_normal_pct: normal_median
                .map(|median| (net_to_zero_pct + sign * median).round_dp(5)),
            net_to_zero_pct,
            quote,
        }
    };
    // 多 a 空 b：入场价差 = b 卖 − a 买，正常基差 m = a − b；回到 m 时还剩 −m 收不回 → +m。
    line.long_a = quote(book_a, book_b, config.size_usdt).map(|q| leg(q, Decimal::ONE));
    line.long_b = quote(book_b, book_a, config.size_usdt).map(|q| leg(q, -Decimal::ONE));
    if line.long_a.is_none() && line.long_b.is_none() {
        line.note = Some(format!("深度不够 {} USDT", config.size_usdt.normalize()));
        return line;
    }
    let score = |leg: &Option<Leg>| {
        leg.as_ref()
            .map(|l| l.net_to_normal_pct.unwrap_or(l.net_to_zero_pct))
    };
    let (direction, chosen) = match (score(&line.long_a), score(&line.long_b)) {
        (Some(x), Some(y)) if y > x => ("long_b", &line.long_b),
        (Some(_), _) => ("long_a", &line.long_a),
        _ => ("long_b", &line.long_b),
    };
    let chosen = chosen.as_ref().expect("至少一个方向有报价");
    let signal = chosen
        .net_to_normal_pct
        .is_some_and(|net| net >= config.alert_net_pct);
    line.best = Some(Best {
        direction,
        signal,
        signal_sec: 0,
        net_usdt: chosen
            .net_to_normal_pct
            .map(|pct| (pct / Decimal::ONE_HUNDRED * config.size_usdt).round_dp(2)),
    });
    line
}

impl Line {
    /// 一个方向的两条腿：(多, 空)。
    pub fn legs(&self, direction: &str) -> Option<(Venue, Venue, &Leg)> {
        match direction {
            "long_a" => Some((self.a, self.b, self.long_a.as_ref()?)),
            "long_b" => Some((self.b, self.a, self.long_b.as_ref()?)),
            _ => None,
        }
    }
}

/// 一组一分钟内的采样（算中位数用）。
#[derive(Default)]
struct Minute {
    start: i64,
    basis: HashMap<String, Vec<f64>>,
    entry_a: HashMap<String, f64>,
    entry_b: HashMap<String, f64>,
    sessions: HashMap<String, Session>,
}

impl Minute {
    fn rows(&self) -> Vec<Row> {
        let mut rows: Vec<Row> = self
            .basis
            .iter()
            .filter(|(_, values)| !values.is_empty())
            .map(|(base, values)| {
                let mut sorted = values.clone();
                sorted.sort_by(f64::total_cmp);
                Row {
                    t: self.start,
                    s: base.clone(),
                    k: self.sessions.get(base).copied().unwrap_or(Session::All),
                    b: (history::percentile(&sorted, 0.5) * 1e5).round() / 1e5,
                    n: u32::try_from(values.len()).unwrap_or(u32::MAX),
                    ea: self.entry_a.get(base).copied(),
                    el: self.entry_b.get(base).copied(),
                }
            })
            .collect();
        rows.sort_by(|a, b| a.s.cmp(&b.s));
        rows
    }
}

/// 从行情 WebSocket 读到的东西，送到计算任务。
enum Feed {
    Book(Venue, feed::Event),
    /// 某家连接状态变了。
    Up(Venue, bool),
}

/// 重建监控行所需的最近一次市场目录。各组历史仍由监控线程独占。
struct Runtime {
    catalog: Catalog,
    identity: Option<pairs::Identity>,
}

/// 一组的运行状态。
struct PairState {
    pair: Pair,
    markets: Vec<PairMarket>,
    /// 上一轮还配得上的合约。拆开后它们不在 `markets` 里，断档仍要接着记。
    watched: HashMap<String, PairMarket>,
    history: history::History,
    minute: Minute,
    /// 正常水平只在每分钟落一行之后才会变：按（合约，时段）缓存，换分钟时清空。
    normals: HashMap<(String, Session), (Option<Normal>, usize)>,
    /// 断档后还不能当开仓目标的合约。只在断档状态变化时更新。
    blocked: HashSet<(String, Session)>,
    note: Option<String>,
}

pub struct Monitor {
    config: Config,
    view: RwLock<View>,
    alerts: Arc<Alerter>,
    /// 最近一分钟发出的价差提醒时刻（[`ALERTS_PER_MINUTE`]）。
    sent: std::sync::Mutex<std::collections::VecDeque<Instant>>,
    /// 实盘还敞口的合约 base。提醒跳过这些币。
    held_bases: HeldBases,
    /// 「不做这些合约」。这些币不生成监控行，也不订阅盘口。
    excluded_bases: std::sync::RwLock<HashSet<String>>,
    runtime: std::sync::Mutex<Option<Runtime>>,
    /// 最近一次成功取到的 Pre-IPO 币名。两个来源合并去重；整次刷新失败时沿用。
    preipo_bases: std::sync::RwLock<HashSet<String>>,
    /// 排除名单变化后立即唤醒监控线程重建。
    rebuild_now: tokio::sync::Notify,
    /// 通知发出时循环可能尚未等待：这个标记保证下一次循环仍会重建。
    rebuild_requested: std::sync::atomic::AtomicBool,
}

/// 订阅变化：每家场所要订阅的市场名（全量）。
type Subscriptions = HashMap<Venue, Vec<String>>;

/// 当前实盘还敞口的合约 base（大写）。价差偏离提醒跳过这些币。
#[derive(Clone, Default)]
pub struct HeldBases {
    inner: Arc<std::sync::Mutex<HashSet<String>>>,
}

impl HeldBases {
    /// 用这一批合约 base 替换跳过名单。空串丢掉；大小写不敏感。
    pub fn replace(&self, bases: impl IntoIterator<Item = impl AsRef<str>>) {
        let next = bases
            .into_iter()
            .map(|base| base.as_ref().trim().to_ascii_uppercase())
            .filter(|base| !base.is_empty())
            .collect();
        *self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = next;
    }

    pub fn contains(&self, base: &str) -> bool {
        let base = base.trim().to_ascii_uppercase();
        !base.is_empty()
            && self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .contains(&base)
    }
}

impl Monitor {
    pub fn new(config: Config, alerts: Arc<Alerter>) -> Arc<Self> {
        Self::with_held(config, alerts, HeldBases::default())
    }

    pub fn with_held(config: Config, alerts: Arc<Alerter>, held_bases: HeldBases) -> Arc<Self> {
        let view = View {
            enabled: config.enabled,
            size_usdt: config.size_usdt,
            alert_net_pct: config.alert_net_pct,
            min_minutes: history::MIN_MINUTES,
            window_days: history::WINDOW_DAYS,
            exclude_preipo: config.exclude_preipo,
            preipo_bases: Vec::new(),
            pairs: config
                .pairs
                .iter()
                .map(|pair| PairView {
                    id: pair.id(),
                    a: pair.a,
                    b: pair.b,
                    markets: 0,
                    history_minutes: 0,
                    fee_min_pct: None,
                    fee_max_pct: None,
                    note: Some("启动中".into()),
                })
                .collect(),
            connected: Connected::default(),
            updated_at: None,
            lines: Vec::new(),
            error: (!config.enabled).then(|| "已关闭（ARB_RH_SPREAD=off）".to_string()),
        };
        Arc::new(Self {
            config,
            view: RwLock::new(view),
            alerts,
            sent: std::sync::Mutex::new(std::collections::VecDeque::new()),
            held_bases,
            excluded_bases: std::sync::RwLock::new(HashSet::new()),
            runtime: std::sync::Mutex::new(None),
            preipo_bases: std::sync::RwLock::new(HashSet::new()),
            rebuild_now: tokio::sync::Notify::new(),
            rebuild_requested: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// 替换「不做这些合约」名单，并立刻重建监控行。空名单不排除。
    pub fn set_excluded_bases(&self, bases: impl IntoIterator<Item = impl AsRef<str>>) {
        let next = bases
            .into_iter()
            .map(|base| base.as_ref().trim().to_ascii_uppercase())
            .filter(|base| !base.is_empty())
            .collect();
        *self
            .excluded_bases
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = next;
        self.rebuild_requested
            .store(true, std::sync::atomic::Ordering::Release);
        self.rebuild_now.notify_one();
    }

    fn store_runtime(&self, catalog: Catalog, identity: Option<pairs::Identity>) {
        *self
            .runtime
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(Runtime { catalog, identity });
    }

    fn update_runtime(&self, catalog: &Catalog, identity: Option<pairs::Identity>) {
        if let Some(runtime) = self
            .runtime
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_mut()
        {
            runtime.catalog.clone_from(catalog);
            runtime.identity = identity;
        }
    }



    pub async fn view(&self) -> View {
        self.view.read().await.clone()
    }

    /// `cache`：扫描缓存，Hyperliquid 的组要用它核对同名合约是不是同一资产。
    pub fn spawn(self: &Arc<Self>, client: reqwest::Client, cache: Arc<crate::cache::ScanCache>) {
        if !self.config.enabled {
            info!("价差监控已关闭");
            return;
        }
        let monitor = Arc::clone(self);
        tokio::spawn(async move { monitor.run(client, cache).await });
    }

    async fn set_error(&self, error: Option<String>) {
        self.view.write().await.error = error;
    }

    /// 用到的场所。
    fn venues(&self) -> Vec<Venue> {
        let mut venues: Vec<Venue> = self
            .config
            .pairs
            .iter()
            .flat_map(|pair| [pair.a, pair.b])
            .collect();
        venues.sort();
        venues.dedup();
        venues
    }

    /// 取一家场所的市场列表。
    async fn catalog_of(
        &self,
        client: &reqwest::Client,
        venue: Venue,
    ) -> anyhow::Result<std::collections::BTreeMap<String, pairs::VenueMarket>> {
        match venue {
            Venue::Arcus => {
                let markets: Value = arb_venues::get_json(
                    client.get("https://api.arcus.xyz/v1/markets"),
                    Venue::Arcus,
                )
                .await?;
                let taker = arb_venues::arcus::fetch_base_taker_fee(client).await?;
                catalog::arcus(&markets, taker)
            }
            Venue::LighterRh => {
                let details: Value = arb_venues::get_json(
                    client.get("https://api.rh.lighter.xyz/api/v1/orderBookDetails"),
                    Venue::LighterRh,
                )
                .await?;
                catalog::lighter(&details)
            }
            Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo => {
                let body = match arb_venues::hyperliquid::dex_name(venue) {
                    Some(dex) => serde_json::json!({"type": "meta", "dex": dex}),
                    None => serde_json::json!({"type": "meta"}),
                };
                let meta: Value = arb_venues::get_json(
                    client.post("https://api.hyperliquid.xyz/info").json(&body),
                    venue,
                )
                .await?;
                catalog::hyperliquid(venue, &meta, self.config.entropy_rebate)
            }
            other => anyhow::bail!("价差监控不支持 {other}"),
        }
    }

    /// 刷新各家市场列表（失败的保留上一次的）。
    async fn refresh_catalog(
        &self,
        client: &reqwest::Client,
        catalog: &mut Catalog,
    ) -> Vec<String> {
        let mut errors = Vec::new();
        for venue in self.venues() {
            match self.catalog_of(client, venue).await {
                Ok(markets) => {
                    catalog.insert(venue, markets);
                }
                Err(error) => {
                    warn!(%venue, "价差监控：取市场列表失败：{error:#}");
                    errors.push(format!("{venue}：{error:#}"));
                }
            }
        }
        errors
    }

    /// 按市场列表与身份簇重算各组的合约；返回每家要订阅的市场名。
    async fn excluded_bases(&self, client: &reqwest::Client) -> HashSet<String> {
        let manual = self
            .excluded_bases
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let lighter = self.lighter_rh_preipo_bases(client).await;
        let hyperliquid = self.hyperliquid_preipo_bases(client).await;
        let mut cached = self
            .preipo_bases
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match (lighter, hyperliquid) {
            (Ok(lighter), Ok(hyperliquid)) => {
                *cached = lighter.into_iter().chain(hyperliquid).collect();
            }
            (Ok(bases), Err(error)) | (Err(error), Ok(bases)) => {
                warn!("有一份 Pre-IPO 名单取失败，沿用上一次完整名单：{error:#}");
                let _ = bases;
            }
            (Err(lighter), Err(hyperliquid)) => {
                warn!("Pre-IPO 名单取失败，沿用上一次：{lighter:#}；{hyperliquid:#}");
            }
        }
        manual.into_iter().chain(cached.iter().cloned()).collect()
    }

    fn preipo_bases(&self) -> HashSet<String> {
        self.preipo_bases
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn cached_excluded_bases(&self) -> HashSet<String> {
        let mut excluded = self
            .excluded_bases
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if self.config.exclude_preipo {
            excluded.extend(self.preipo_bases());
        }
        excluded
    }

    async fn lighter_rh_preipo_bases(
        &self,
        client: &reqwest::Client,
    ) -> anyhow::Result<HashSet<String>> {
        let page = client
            .get(catalog::LIGHTER_APP_URL)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let script = client
            .get(catalog::lighter_token_script_url(&page)?)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        Ok(catalog::lighter_rh_preipo_bases(&script)?
            .into_iter()
            .collect())
    }

    async fn hyperliquid_preipo_bases(
        &self,
        client: &reqwest::Client,
    ) -> anyhow::Result<HashSet<String>> {
        let rows: Value = arb_venues::get_json(
            client
                .post("https://api.hyperliquid.xyz/info")
                .json(&serde_json::json!({"type": "perpCategories"})),
            Venue::Hyperliquid,
        )
        .await?;
        Ok(catalog::hyperliquid_preipo_bases(&rows)?.into_iter().collect())
    }

    fn rebuild(
        &self,
        states: &mut [PairState],
        catalog: &Catalog,
        identity: Option<&pairs::Identity>,
        excluded: &HashSet<String>,
    ) -> Subscriptions {
        let mut subs: HashMap<Venue, std::collections::BTreeSet<String>> = HashMap::new();
        for state in states.iter_mut() {
            let pair = state.pair;
            let fresh = pairs::pair_markets(pair, catalog, identity, self.config.equities_only)
                .into_iter()
                .filter(|market| !excluded.contains(&market.base))
                .collect::<Vec<_>>();
            if pair.needs_identity_check() {
                let still: HashSet<String> = fresh.iter().map(|market| market.base.clone()).collect();
                state.watched.retain(|base, _| !still.contains(base));
                for market in &fresh {
                    state.watched.insert(market.base.clone(), market.clone());
                }
            }
            state.markets = fresh;
            state.note = if !catalog.contains_key(&pair.a) || !catalog.contains_key(&pair.b) {
                Some("有一家的市场列表还没取到".into())
            } else if pair.needs_identity_check() && identity.is_none() {
                Some("等首轮扫描核对同名合约是不是同一资产".into())
            } else if state.markets.is_empty() {
                Some("两家没有核实过的同名合约".into())
            } else {
                None
            };
            for market in &state.markets {
                subs.entry(pair.a).or_default().insert(market.a.key.clone());
                subs.entry(pair.b).or_default().insert(market.b.key.clone());
            }
        }
        subs.into_iter()
            .map(|(venue, keys)| (venue, keys.into_iter().collect()))
            .collect()
    }

    /// 需要身份检查的组：不在同一簇就不记这一分钟。跨到下一次时段仍断着，才作废旧样本。
    async fn note_breaks(
        &self,
        state: &mut PairState,
        identity: Option<&pairs::Identity>,
        now: DateTime<Utc>,
    ) {
        let Some(identity) = identity else {
            return;
        };
        let pair = state.pair;
        let minute = now.timestamp() / 60 * 60;
        for market in state.watched.values() {
            let session = classify(now, market.crypto(), market.outside_rth);
            let same = identity
                .get(&(pair.a, market.base.clone()))
                .zip(identity.get(&(pair.b, market.base.clone())))
                .is_some_and(|(a, b)| a == b);
            let step = session::note_break(
                &self.config.dir,
                &pair.id(),
                &market.base,
                session,
                minute,
                !same,
            );
            let key = (market.base.clone(), session);
            if step.blocked_until.is_some() {
                state.blocked.insert(key);
            } else {
                state.blocked.remove(&key);
            }
            let Some(cutoff) = step.invalidate_before else {
                continue;
            };
            state
                .history
                .invalidate(&market.base, session, cutoff.timestamp());
            state.normals.remove(&(market.base.clone(), session));
            if let Err(error) = history::invalidate(
                &self.config.dir,
                &pair.file_prefix(),
                &market.base,
                session,
                cutoff.timestamp(),
                now,
            )
            .await
            {
                warn!(pair = %pair.id(), base = %market.base, "断档前的价差样本删不掉：{error}");
            }
        }
    }

    async fn run(self: Arc<Self>, client: reqwest::Client, cache: Arc<crate::cache::ScanCache>) {
        let mut catalog = Catalog::new();
        loop {
            let errors = self.refresh_catalog(&client, &mut catalog).await;
            if !catalog.is_empty() {
                self.set_error((!errors.is_empty()).then(|| errors.join("；")))
                    .await;
                break;
            }
            self.set_error(Some(format!(
                "取市场列表失败，60 秒后重试：{}",
                errors.join("；")
            )))
            .await;
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        let now = Utc::now();
        let mut states = Vec::new();
        for &pair in &self.config.pairs {
            let (mut history, broken) =
                history::load(&self.config.dir, &pair.file_prefix(), now).await;
            if broken > 0 {
                warn!(pair = %pair.id(), broken, "价差历史里有坏行，已跳过");
            }
            history.prune(now);
            states.push(PairState {
                pair,
                markets: Vec::new(),
                watched: HashMap::new(),
                history,
                minute: Minute {
                    start: now.timestamp() / 60 * 60,
                    ..Minute::default()
                },
                normals: HashMap::new(),
                blocked: session::blocked_now(&self.config.dir, &pair.id(), now)
                    .into_iter()
                    .collect(),
                note: None,
            });
        }
        let identity = cache
            .get()
            .await
            .map(|snap| pairs::identity_of(&snap.report));
        let excluded = self.excluded_bases(&client).await;
        let mut subs = self.rebuild(&mut states, &catalog, identity.as_ref(), &excluded);
        self.store_runtime(catalog.clone(), identity.clone());
        let mut identity_ready = identity.is_some();
        info!(
            pairs = %self.config.pairs.iter().map(|p| p.id()).collect::<Vec<_>>().join(","),
            markets = states.iter().map(|s| s.markets.len()).sum::<usize>(),
            "价差监控启动"
        );

        let (tx, mut rx) = mpsc::channel::<Feed>(8192);
        // 每家一个连接任务；订阅列表变化时通过 watch 通知它重连。Lighter 另有一个「重订阅某个市场」的通道。
        let mut senders: HashMap<Venue, tokio::sync::watch::Sender<Vec<String>>> = HashMap::new();
        let (resub_tx, resub_rx) = mpsc::channel::<String>(64);
        let mut resub_rx = Some(resub_rx);
        for venue in self.venues() {
            let (sub_tx, sub_rx) =
                tokio::sync::watch::channel(subs.get(&venue).cloned().unwrap_or_default());
            senders.insert(venue, sub_tx);
            let resub = if venue == Venue::LighterRh {
                resub_rx.take()
            } else {
                None
            };
            tokio::spawn(venue_loop(venue, sub_rx, resub, tx.clone()));
        }

        // 盘口：(场所, 市场名) → 本地盘口。几组共用同一家的盘口。
        let mut books: HashMap<(Venue, String), LocalBook> = HashMap::new();
        let mut connected = Connected::default();
        for venue in self.venues() {
            connected.venues.insert(venue, false);
        }
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut refresh = tokio::time::interval(Duration::from_secs(300));
        refresh.tick().await;
        let mut next_preipo_refresh = Instant::now() + Duration::from_secs(6 * 60 * 60);
        // 信号从什么时候开始连续成立（组, 合约, 方向）。
        let mut signal_since: HashMap<(String, String, &'static str), Instant> = HashMap::new();

        loop {
            tokio::select! {
                Some(message) = rx.recv() => match message {
                    Feed::Up(venue, up) => {
                        connected.venues.insert(venue, up);
                        if !up {
                            connected.reconnects += 1;
                            // 断线后旧盘口不能再用：等重连后的快照。
                            books.retain(|(v, _), _| *v != venue);
                        }
                    }
                    Feed::Book(venue, event) => {
                        let resub = (venue == Venue::LighterRh).then_some(&resub_tx);
                        apply(&mut books, venue, event, resub);
                    }
                },
                _ = self.rebuild_now.notified(), if self.rebuild_requested.load(std::sync::atomic::Ordering::Acquire) => {
                    self.rebuild_requested.store(false, std::sync::atomic::Ordering::Release);
                    let (fresh_catalog, fresh_identity) = self
                        .runtime
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .as_ref()
                        .map(|runtime| (runtime.catalog.clone(), runtime.identity.clone()))
                        .unwrap_or_default();
                    if !fresh_catalog.is_empty() {
                        catalog = fresh_catalog;
                        let excluded = self.cached_excluded_bases();
                        let fresh = self.rebuild(&mut states, &catalog, fresh_identity.as_ref(), &excluded);
                        if fresh != subs {
                            subs = fresh;
                            for (venue, sender) in &senders {
                                let list = subs.get(venue).cloned().unwrap_or_default();
                                if *sender.borrow() != list {
                                    let _ = sender.send(list);
                                }
                            }
                        }
                    }
                }
                _ = refresh.tick() => {
                    // 交易时段（isOutsideRth）、费率、上下架会变：5 分钟刷新一次。
                    // Pre-IPO 分类变化很慢：到期后搭这趟请求，不再单独开一轮。
                    let _ = self.refresh_catalog(&client, &mut catalog).await;
                    let identity = cache.get().await.map(|snap| pairs::identity_of(&snap.report));
                    identity_ready |= identity.is_some();
                    let excluded = if Instant::now() >= next_preipo_refresh {
                        next_preipo_refresh = Instant::now() + Duration::from_secs(6 * 60 * 60);
                        self.excluded_bases(&client).await
                    } else {
                        self.cached_excluded_bases()
                    };
                    let fresh = self.rebuild(&mut states, &catalog, identity.as_ref(), &excluded);
                    self.update_runtime(&catalog, identity);
                    if fresh != subs {
                        subs = fresh;
                        for (venue, sender) in &senders {
                            let list = subs.get(venue).cloned().unwrap_or_default();
                            if *sender.borrow() != list {
                                let _ = sender.send(list);
                            }
                        }
                    }
                }
                _ = tick.tick() => {
                    if crate::shutdown::is_draining() {
                        // 停机：把这一分钟写掉再退出循环。
                        for state in &states {
                            let _ = history::append(&self.config.dir, &state.pair.file_prefix(), &state.minute.rows(), Utc::now()).await;
                        }
                        return;
                    }
                    // 首轮扫描刚出来：Hyperliquid 的组现在能核对身份了，不等 5 分钟。
                    if !identity_ready
                        && let Some(snap) = cache.get().await
                    {
                        identity_ready = true;
                        let identity = pairs::identity_of(&snap.report);
                        let excluded = self.cached_excluded_bases();
                        subs = self.rebuild(&mut states, &catalog, Some(&identity), &excluded);
                        self.update_runtime(&catalog, Some(identity));
                        for (venue, sender) in &senders {
                            let _ = sender.send(subs.get(venue).cloned().unwrap_or_default());
                        }
                    }
                    let now_utc = Utc::now();
                    let now = Instant::now();
                    let start = now_utc.timestamp() / 60 * 60;
                    let mut lines = Vec::new();
                    let mut break_identity = None;
                    for state in &mut states {
                        let pair = state.pair;
                        if start != state.minute.start {
                            let rows = state.minute.rows();
                            rows.iter().for_each(|row| state.history.push(row));
                            state.history.prune(now_utc);
                            if let Err(error) = history::append(&self.config.dir, &pair.file_prefix(), &rows, now_utc).await {
                                warn!(pair = %pair.id(), "价差历史写不进去：{error}");
                            }
                            state.minute = Minute { start, ..Minute::default() };
                            state.normals.clear();
                        }
                        if pair.needs_identity_check() && state.minute.start == start && state.minute.basis.is_empty() {
                            if break_identity.is_none() {
                                break_identity = Some(cache.get().await.map(|snap| pairs::identity_of(&snap.report)));
                            }
                            self.note_breaks(state, break_identity.as_ref().and_then(Option::as_ref), now_utc).await;
                        }
                        for market in &state.markets {
                            let session = classify(now_utc, market.crypto(), market.outside_rth);
                            let history = &state.history;
                            let (normal, normal_minutes) = state
                                .normals
                                .entry((market.base.clone(), session))
                                .or_insert_with(|| {
                                    (history.normal(&market.base, session, now_utc), history.minutes(&market.base, session, now_utc))
                                })
                                .clone();
                            let mut line = evaluate(
                                pair,
                                market,
                                books.get(&(pair.a, market.a.key.clone())),
                                books.get(&(pair.b, market.b.key.clone())),
                                normal,
                                normal_minutes,
                                session,
                                &self.config,
                                now,
                            );
                            line.normal_blocked = state.blocked.contains(&(market.base.clone(), session));
                            let id = pair.id();
                            if let Some(best) = line.best.as_mut() {
                                let key = (id.clone(), market.base.clone(), best.direction);
                                if best.signal {
                                    let since = *signal_since.entry(key).or_insert(now);
                                    best.signal_sec = now.saturating_duration_since(since).as_secs();
                                } else {
                                    signal_since.remove(&key);
                                }
                            }
                            // 方向换了或没信号：另一个方向的计时作废。
                            signal_since.retain(|(p, base, direction), _| {
                                p != &id
                                    || base != &market.base
                                    || line.best.as_ref().is_some_and(|b| b.signal && b.direction == *direction)
                            });
                            if let Some(basis) = line.basis_pct.and_then(|b| b.to_f64()) {
                                let minute = &mut state.minute;
                                minute.basis.entry(market.base.clone()).or_default().push(basis);
                                minute.sessions.insert(market.base.clone(), session);
                                let keep_max = |map: &mut HashMap<String, f64>, value: Option<f64>| {
                                    if let Some(value) = value {
                                        let slot = map.entry(market.base.clone()).or_insert(value);
                                        *slot = slot.max(value);
                                    }
                                };
                                keep_max(&mut minute.entry_a, line.long_a.as_ref().and_then(|l| l.quote.entry_pct.to_f64()));
                                keep_max(&mut minute.entry_b, line.long_b.as_ref().and_then(|l| l.quote.entry_pct.to_f64()));
                            }
                            self.maybe_alert(&line);
                            lines.push(line);
                        }
                    }
                    lines.sort_by(|a, b| {
                        let key = |l: &Line| l.best.as_ref().and_then(|b| b.net_usdt).unwrap_or(Decimal::MIN);
                        key(b).cmp(&key(a)).then_with(|| a.pair.cmp(&b.pair)).then_with(|| a.base.cmp(&b.base))
                    });
                    let pair_views: Vec<PairView> = states
                        .iter()
                        .map(|state| {
                            let fees: Vec<Decimal> = state.markets.iter().filter_map(PairMarket::round_trip_pct).collect();
                            PairView {
                                id: state.pair.id(),
                                a: state.pair.a,
                                b: state.pair.b,
                                markets: state.markets.len(),
                                history_minutes: state.history.coverage_minutes(),
                                fee_min_pct: fees.iter().min().copied(),
                                fee_max_pct: fees.iter().max().copied(),
                                note: state.note.clone(),
                            }
                        })
                        .collect();
                    let mut view = self.view.write().await;
                    view.connected = connected.clone();
                    view.exclude_preipo = self.config.exclude_preipo;
                    view.preipo_bases = {
                        let mut bases: Vec<String> = self.preipo_bases().into_iter().collect();
                        bases.sort();
                        bases
                    };
                    view.pairs = pair_views;
                    view.updated_at = Some(now_utc);
                    view.lines = lines;
                }
            }
        }
    }

    fn maybe_alert(&self, line: &Line) {
        let Some(best) = line
            .best
            .as_ref()
            .filter(|b| b.signal && b.signal_sec >= SIGNAL_HOLD.as_secs())
        else {
            return;
        };
        if self.held_bases.contains(&line.base) {
            debug!(symbol = %line.base, "价差偏离：已有持仓，不推送");
            return;
        }
        let Some(text) = alert_text(line, best, &self.config) else {
            return;
        };
        let now = Instant::now();
        let mut sent = self
            .sent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while sent
            .front()
            .is_some_and(|at| now.duration_since(*at) >= Duration::from_secs(60))
        {
            sent.pop_front();
        }
        if sent.len() >= ALERTS_PER_MINUTE {
            return;
        }
        // 最早那组沿用原来的告警 key（冷却不因升级重置）。
        let key = if line.pair == Pair::RH.id() {
            let legacy = if best.direction == "long_a" {
                "long_arcus"
            } else {
                "long_lighter"
            };
            format!("rh-spread:{}:{legacy}", line.base)
        } else {
            format!("spread:{}:{}:{}", line.pair, line.base, best.direction)
        };
        if self.alerts.notify_telegram(&key, text) {
            sent.push_back(now);
            info!(pair = %line.pair, symbol = %line.base, direction = best.direction, net_usdt = ?best.net_usdt, "价差监控：推送提醒");
        }
    }
}

/// 场所的显示名（提醒、页面用）。
pub fn venue_label(venue: Venue) -> &'static str {
    match venue {
        Venue::Arcus => "Arcus",
        Venue::LighterRh => "Lighter RH",
        Venue::Hyperliquid => "Hyperliquid",
        Venue::HyperliquidXyz => "HL-xyz",
        Venue::HyperliquidIo => "HL-io",
        other => other.as_str(),
    }
}

/// 提醒文本。
pub fn alert_text(line: &Line, best: &Best, config: &Config) -> Option<String> {
    let (long, short, leg) = line.legs(best.direction)?;
    let normal = line.normal.as_ref()?;
    Some(format!(
        "📈 价差偏离：{} {}，多 {} / 空 {}\n可成交价差 {}%（{} USDT），{}正常基差 {:.3}%，当前 {}%（{} − {}）\n回到正常水平预估净赚 {}%（≈{} USDT，已扣手续费与平仓穿价）\n提醒；是否自动下单见面板的自动交易设置。同一合约同一方向 30 分钟内不重复。",
        line.base,
        line.session.label(),
        venue_label(long),
        venue_label(short),
        leg.quote.entry_pct.round_dp(3),
        config.size_usdt.normalize(),
        line.session.label(),
        normal.median,
        line.basis_pct?.round_dp(3),
        venue_label(line.a),
        venue_label(line.b),
        leg.net_to_normal_pct?.round_dp(3),
        best.net_usdt?.normalize(),
    ))
}

fn apply(
    books: &mut HashMap<(Venue, String), LocalBook>,
    venue: Venue,
    event: feed::Event,
    resub: Option<&mpsc::Sender<String>>,
) {
    let now = Instant::now();
    match event {
        feed::Event::Snapshot {
            market,
            bids,
            asks,
            nonce,
        } => {
            let mut book = LocalBook::new(now);
            bids.into_iter()
                .for_each(|(p, q)| LocalBook::apply(&mut book.bids, p, q));
            asks.into_iter()
                .for_each(|(p, q)| LocalBook::apply(&mut book.asks, p, q));
            book.nonce = nonce;
            books.insert((venue, market), book);
        }
        feed::Event::Delta {
            market,
            bids,
            asks,
            begin_nonce,
            nonce,
        } => {
            let key = (venue, market);
            let Some(book) = books.get_mut(&key) else {
                return;
            };
            if let (Some(last), Some(begin)) = (book.nonce, begin_nonce)
                && last != begin
            {
                // 丢了增量：这本盘口不能再信，作废并重订阅拿新快照。
                debug!(market = %key.1, last, begin, "Lighter 盘口增量不连续，重订阅");
                books.remove(&key);
                if let Some(resub) = resub {
                    let _ = resub.try_send(key.1);
                }
                return;
            }
            bids.into_iter()
                .for_each(|(p, q)| LocalBook::apply(&mut book.bids, p, q));
            asks.into_iter()
                .for_each(|(p, q)| LocalBook::apply(&mut book.asks, p, q));
            book.nonce = nonce.or(book.nonce);
            book.updated = now;
        }
        feed::Event::Error(message) => warn!(%venue, "价差监控：行情服务端报错：{message}"),
        feed::Event::Other => {}
    }
}

// ───────────────────────────── WebSocket 连接 ─────────────────────────────

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(url: &str) -> anyhow::Result<Ws> {
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(15),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .map_err(|_| anyhow::anyhow!("连接超时"))??;
    Ok(ws)
}

/// 重连等待：1、2、4 … 秒，封顶 60 秒；连上并稳定 60 秒后复位。
fn backoff(failures: u32) -> Duration {
    Duration::from_secs(1u64 << failures.min(6)).min(Duration::from_secs(60))
}

/// 一家场所的协议细节。
struct Protocol {
    url: &'static str,
    subscribe: fn(&str) -> Option<String>,
    unsubscribe: fn(&str) -> Option<String>,
    parse: fn(&str) -> Result<feed::Event, String>,
    /// 应用层心跳文本；`None` = 用 WebSocket 协议层 ping。
    ping: Option<&'static str>,
}

fn protocol(venue: Venue) -> Option<Protocol> {
    match venue {
        Venue::LighterRh => Some(Protocol {
            url: feed::LIGHTER_WS,
            subscribe: |key| key.parse().ok().map(feed::lighter_subscribe),
            unsubscribe: |key| key.parse().ok().map(feed::lighter_unsubscribe),
            parse: feed::parse_lighter,
            // 2 分钟内必须发一帧。
            ping: Some(r#"{"type":"ping"}"#),
        }),
        Venue::Arcus => Some(Protocol {
            url: feed::ARCUS_WS,
            subscribe: |key| Some(feed::arcus_subscribe(key)),
            unsubscribe: |_| None,
            parse: feed::parse_arcus,
            // Arcus 连接 24 小时自动断，断了就重连。
            ping: None,
        }),
        Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo => Some(Protocol {
            url: feed::HYPERLIQUID_WS,
            subscribe: |key| Some(feed::hyperliquid_subscribe(key)),
            unsubscribe: |_| None,
            parse: feed::parse_hyperliquid,
            // 60 秒没有消息服务端会断开；订阅了就一直有推送，另发应用层 ping 兜底。
            ping: Some(r#"{"method":"ping"}"#),
        }),
        _ => None,
    }
}

/// 一家场所的行情连接：断线指数退避重连；订阅列表变了就重连（全量重订阅最简单，也不会漏）。
async fn venue_loop(
    venue: Venue,
    mut subs: tokio::sync::watch::Receiver<Vec<String>>,
    mut resub: Option<mpsc::Receiver<String>>,
    tx: mpsc::Sender<Feed>,
) {
    let Some(protocol) = protocol(venue) else {
        return;
    };
    let mut failures = 0u32;
    loop {
        if crate::shutdown::is_draining() {
            return;
        }
        let keys = subs.borrow_and_update().clone();
        if keys.is_empty() {
            // 这家暂时没有要订阅的（等首轮扫描核对身份）：等订阅列表变化。
            if subs.changed().await.is_err() {
                return;
            }
            continue;
        }
        let started = Instant::now();
        match session(venue, &protocol, &keys, &mut subs, resub.as_mut(), &tx).await {
            Ok(Some(())) => {
                // 订阅列表变了：马上用新列表重连。
                let _ = tx.send(Feed::Up(venue, false)).await;
                failures = 0;
            }
            Ok(None) => return,
            Err(error) => {
                let _ = tx.send(Feed::Up(venue, false)).await;
                if started.elapsed() > Duration::from_secs(60) {
                    failures = 0;
                }
                let wait = backoff(failures);
                failures += 1;
                warn!(
                    "价差监控：{venue} 行情断开，{} 秒后重连：{error:#}",
                    wait.as_secs()
                );
                tokio::time::sleep(wait).await;
            }
        }
    }
}

/// 一次连接。`Ok(Some(()))` = 订阅列表变了要重连；`Ok(None)` = 停机或下游没了。
async fn session(
    venue: Venue,
    protocol: &Protocol,
    keys: &[String],
    subs: &mut tokio::sync::watch::Receiver<Vec<String>>,
    mut resub: Option<&mut mpsc::Receiver<String>>,
    tx: &mpsc::Sender<Feed>,
) -> anyhow::Result<Option<()>> {
    let mut ws = connect(protocol.url).await?;
    // Lighter 每分钟最多 200 条客户端消息、Hyperliquid 每条连接最多 1000 个订阅：几十个一次发完没问题。
    for key in keys {
        if let Some(text) = (protocol.subscribe)(key) {
            ws.send(Message::text(text)).await?;
        }
    }
    let _ = tx.send(Feed::Up(venue, true)).await;
    let mut ping = tokio::time::interval(Duration::from_secs(30));
    ping.tick().await;
    let mut last_resub: HashMap<String, Instant> = HashMap::new();
    loop {
        tokio::select! {
            message = tokio::time::timeout(STALE * 2, ws.next()) => {
                let message = message.map_err(|_| anyhow::anyhow!("{} 秒没收到任何消息", (STALE * 2).as_secs()))?;
                match message {
                    Some(Ok(Message::Text(text))) => match (protocol.parse)(&text) {
                        Ok(event) => { if tx.send(Feed::Book(venue, event)).await.is_err() { return Ok(None); } }
                        Err(error) => debug!(%venue, "行情消息解析失败：{error}"),
                    },
                    Some(Ok(Message::Ping(payload))) => ws.send(Message::Pong(payload)).await?,
                    Some(Ok(Message::Close(frame))) => anyhow::bail!("服务端关闭连接：{frame:?}"),
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error.into()),
                    None => anyhow::bail!("连接结束"),
                }
            }
            changed = subs.changed() => {
                if changed.is_err() { return Ok(None); }
                let _ = ws.close(None).await;
                return Ok(Some(()));
            }
            Some(key) = async {
                match resub.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                // 同一个市场 10 秒内最多重订阅一次。
                if last_resub.get(&key).is_none_or(|at| at.elapsed() > Duration::from_secs(10)) {
                    last_resub.insert(key.clone(), Instant::now());
                    if let Some(text) = (protocol.unsubscribe)(&key) { ws.send(Message::text(text)).await?; }
                    if let Some(text) = (protocol.subscribe)(&key) { ws.send(Message::text(text)).await?; }
                }
            }
            _ = ping.tick() => {
                if crate::shutdown::is_draining() { let _ = ws.close(None).await; return Ok(None); }
                match protocol.ping {
                    Some(text) => ws.send(Message::text(text)).await?,
                    None => ws.send(Message::Ping(Vec::new().into())).await?,
                }
            }
        }
    }
}
