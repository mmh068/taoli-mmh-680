//! 追加式台账。
//!
//! 为什么是**追加式**而不是「最新状态覆盖」：执行层最需要回答的问题是「当时到底
//! 发生了什么」。覆盖式存储只能告诉你当前状态，而事故总是发生在状态转移的缝里
//! —— 「第一腿成交了、第二腿超时」这种缝，覆盖式一写就没了。
//!
//! 格式是 JSONL：一行一个记录，可以直接 `tail`、`grep`、灌进任何工具，不需要
//! 数据库驱动。行数不多（每笔交易几条），换来的可审计性远比省下的空间值钱。
//!
//! 查询最新状态不每次把整份历史再解析一遍。旁边的 `{台账路径}.snapshot.json`
//! 记下仍要用的订单和仓位的最新值，以及这份快照覆盖到台账的哪个字节。之后的查询
//! 读快照，再只补其后新追加的行。台账本身仍然只追加；一条仓位的全过程
//! （[`Ledger::position_history`]）继续从原文件翻。
//!
//! 快照不是第二本台账，压实时会丢掉已经用不上的记录，所以文件不会跟着运行时间一直变大：
//! 仓位留下有敞口的、已结束但还没结算清楚的、近 7 个北京日结束的，以及按开仓时间最近的
//! 64 笔已结束仓位。订单留下近 7 日见过的、仍挂着的，以及上面那些还没结算完的仓位名下的。

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, SeekFrom};
use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::types::{OrderState, PairPosition, PositionStatus};

const SNAPSHOT_VERSION: u32 = 1;
/// 新增部分超过这么多字节，就把最新状态再收成一份快照。
const COMPACT_AFTER: u64 = 256 * 1024;
/// 快照里保留的已结束仓位天数（北京日历日，含今天）。订单也按这个窗口留。
const KEEP_DAYS: i64 = 7;
/// 交易很少时也留下最近这么多笔已结束仓位，供持仓页和连续回滚熔断使用。
const KEEP_RECENT_ENDED: usize = 64;

#[derive(Debug, Serialize, Deserialize)]
struct LedgerSnapshot {
    version: u32,
    /// 快照覆盖的台账字节数，落在某一行换行之后。
    through: u64,
    broken: usize,
    orders: HashMap<String, OrderState>,
    positions: HashMap<String, PairPosition>,
    /// 订单最近一次出现在台账尾部的时刻。旧快照没有这个字段。
    #[serde(default)]
    order_at: HashMap<String, DateTime<Utc>>,
}

/// 台账里的一行。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Record {
    Order(Box<OrderState>),
    Position(Box<PairPosition>),
}

impl Record {
    /// 这一行的主键。重放时按它取最新值。
    pub fn key(&self) -> &str {
        match self {
            Record::Order(order) => &order.order.client_order_id.0,
            Record::Position(position) => &position.id,
        }
    }
}

/// 重放结果：每个主键的最新状态。
#[derive(Debug, Default, Clone)]
pub struct Replayed {
    pub orders: HashMap<String, OrderState>,
    pub positions: HashMap<String, PairPosition>,
}

impl Replayed {
    /// 还有敞口的仓位。对账与风控只看这些。
    pub fn exposed(&self) -> Vec<&PairPosition> {
        self.positions
            .values()
            .filter(|position| position.status.has_exposure())
            .collect()
    }
}

pub struct Ledger {
    path: PathBuf,
    file: Mutex<tokio::fs::File>,
}

impl Ledger {
    /// 打开（不存在则创建）台账文件。目录会被自动创建。
    pub async fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        // 崩溃可能留下**半行**（写到一半断电）。`append` 在 `sync_data` 之后才算成功，所以半行
        // 从来不是一条已确认的记录：调用方没有拿到 `Ok`，订单意图没发出去，仓位记录也还停在
        // 上一条。把它挪进 `.torn` 旁路文件（留证）并从台账里截掉 —— 不能只补个换行封口：
        // 封口后它就成了一条永久解析不了的坏行，对账看见坏行会拒绝一切开仓与规则。
        // 例外：只缺换行的完整记录（崩溃正好断在换行之前）照常保留，补上换行。
        // 只读末尾这一行，不把整本台账读进来。
        match incomplete_tail(&path).await {
            Ok(Some((start, tail))) => {
                if serde_json::from_slice::<Record>(&tail).is_ok() {
                    let mut repair = tokio::fs::OpenOptions::new()
                        .append(true)
                        .open(&path)
                        .await?;
                    repair.write_all(b"\n").await?;
                    repair.sync_data().await?;
                } else {
                    let mut sidecar = tokio::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(torn_path(&path))
                        .await?;
                    sidecar.write_all(&tail).await?;
                    sidecar.write_all(b"\n").await?;
                    sidecar.sync_data().await?;
                    let truncate = tokio::fs::OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .await?;
                    truncate.set_len(start).await?;
                    truncate.sync_data().await?;
                    tracing::warn!(
                        path = %path.display(),
                        bytes = tail.len(),
                        "台账末尾有写到一半的记录（上次崩溃的现场）：已移到 .torn 旁路文件并从台账截掉"
                    );
                }
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "检查台账末尾的半行失败，这次不改文件");
            }
        }

        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 追加一行并调用 `sync_data` 持久化；订单意图必须在网络提交前落盘。
    pub async fn append(&self, record: &Record) -> std::io::Result<()> {
        let mut line = serde_json::to_string(record)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        line.push('\n');
        let mut file = self.file.lock().await;
        file.write_all(line.as_bytes()).await?;
        file.sync_data().await?;
        if let Record::Position(position) = record {
            if let Err(error) = crate::verify_queue::note_position(self.path(), position) {
                tracing::warn!(%error, "更新待核实仓位名单失败");
            }
        }
        Ok(())
    }

    /// 这个仓位 id 的全部历史记录（最旧在前）。结束后的仓位已经清掉了两条腿，事后要拿
    /// 开仓时的腿（价格、数量）就得回头翻它。
    pub async fn position_history(&self, id: &str) -> std::io::Result<Vec<PairPosition>> {
        let content = read_complete_lines(&self.path).await?;
        Ok(content
            .lines()
            .filter_map(|line| serde_json::from_str::<Record>(line).ok())
            .filter_map(|record| match record {
                Record::Position(position) if position.id == id => Some(*position),
                _ => None,
            })
            .collect())
    }

    /// 重放整个台账。坏行只跳过并计数，不让一行脏数据毁掉整份历史。
    pub async fn replay(&self) -> std::io::Result<(Replayed, usize)> {
        replay_file(&self.path).await
    }
}

/// 只读地重放一份台账文件：不创建、不修补、不追加。
///
/// 看板用它展示持仓。[`Ledger::open`] 会创建文件并给半行补换行，那是写操作 ——
/// 只读的一方不该改动执行方正在追加的文件。文件不存在时返回空结果。
/// 有快照时不重读它已经覆盖的前缀；快照写在旁边的文件里，不改台账本身。
pub async fn replay_file(path: impl AsRef<Path>) -> std::io::Result<(Replayed, usize)> {
    let path = path.as_ref();
    let file_len = match tokio::fs::metadata(path).await {
        Ok(meta) => meta.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Replayed::default(), 0));
        }
        Err(error) => return Err(error),
    };
    let snap = load_snapshot(path, file_len).await;
    let start = snap.as_ref().map(|snap| snap.through).unwrap_or(0);
    let (text, covered) = read_range(path, start).await?;
    let mut broken = snap.as_ref().map(|snap| snap.broken).unwrap_or(0);
    let mut order_at = snap
        .as_ref()
        .map(|snap| snap.order_at.clone())
        .unwrap_or_default();
    let mut replayed = snap
        .map(|snap| Replayed {
            orders: snap.orders,
            positions: snap.positions,
        })
        .unwrap_or_default();
    let folded = fold_lines(&mut replayed, &text);
    broken += folded.broken;
    let now = Utc::now();
    stamp_orders(&replayed, &mut order_at, &folded.order_ids, start > 0, now);
    let (orders, positions, order_at) = prune_snapshot(&replayed, &order_at, now);
    let tail = covered.saturating_sub(start);
    // 尾部够长就前移水位。旧快照里已经用不上的记录，下一次查询就收掉，不必再等 256 KiB。
    if tail >= COMPACT_AFTER
        || orders.len() < replayed.orders.len()
        || positions.len() < replayed.positions.len()
    {
        // 先把终身合计记进滚动存档，再把旧行从快照里拿掉。这次返回的仍是折叠后的全部状态；
        // 下一次查询才只看到留下的部分。
        crate::cumulative::archive_before_snapshot_prune(path, &replayed);
        crate::window::note_replay(path, &replayed);
        let snapshot = LedgerSnapshot {
            version: SNAPSHOT_VERSION,
            through: covered,
            broken,
            orders,
            positions,
            order_at,
        };
        if let Err(error) = write_snapshot(path, &snapshot).await {
            tracing::warn!(
                path = %path.display(),
                "台账快照没有写上，下次仍从上一份补：{error}"
            );
        }
    }
    Ok((replayed, broken))
}

/// 忽略快照，把整本台账折成最新状态。
///
/// 页面查询走 [`replay_file`]，不读已经盖住的前缀。累计存档补交易所行时，前缀里的旧仓位还要用，只在这一次补记里读整本。
pub(crate) fn replay_full_sync(path: &Path) -> std::io::Result<Replayed> {
    let bytes = std::fs::read(path)?;
    // 末尾没换行的一段还没落定，和页面重放一样不解析。
    let end = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |at| at + 1);
    let text = String::from_utf8_lossy(&bytes[..end]).into_owned();
    let mut replayed = Replayed::default();
    fold_lines(&mut replayed, &text);
    Ok(replayed)
}

/// 只留这一笔仓位的订单。先用已经加载的回放；里面没有开仓单，再逐行扫台账，不把整本折进内存。
pub(crate) fn orders_for_position(
    path: &Path,
    snapshot: Option<&Replayed>,
    position_id: &str,
) -> Replayed {
    if snapshot.is_some_and(|replayed| has_opening_order(replayed, position_id)) {
        return copy_position_orders(snapshot.expect("上面已判断"), position_id);
    }
    scan_position_orders(path, position_id)
}

fn has_opening_order(replayed: &Replayed, position_id: &str) -> bool {
    replayed
        .orders
        .keys()
        .any(|id| is_opening_order(id, position_id))
}

fn is_opening_order(order_id: &str, position_id: &str) -> bool {
    if position_id_from_order(order_id) != Some(position_id) {
        return false;
    }
    order_id
        .rsplit_once('-')
        .is_some_and(|(_, tail)| !tail.is_empty() && tail.chars().all(|ch| ch.is_ascii_digit()))
}

fn copy_position_orders(replayed: &Replayed, position_id: &str) -> Replayed {
    let mut out = Replayed::default();
    for (id, order) in &replayed.orders {
        if position_id_from_order(id) == Some(position_id) {
            out.orders.insert(id.clone(), order.clone());
        }
    }
    out
}

fn scan_position_orders(path: &Path, position_id: &str) -> Replayed {
    let mut out = Replayed::default();
    let Ok(file) = std::fs::File::open(path) else {
        return out;
    };
    let mut reader = BufReader::new(file);
    let mut buf = Vec::new();
    let buy = format!("{position_id}-buy-");
    let sell = format!("{position_id}-sell-");
    loop {
        buf.clear();
        let Ok(n) = reader.read_until(b'\n', &mut buf) else {
            break;
        };
        if n == 0 || !buf.ends_with(b"\n") {
            break;
        }
        if !buf.windows(buy.len()).any(|window| window == buy.as_bytes())
            && !buf.windows(sell.len()).any(|window| window == sell.as_bytes())
        {
            continue;
        }
        let Ok(line) = std::str::from_utf8(&buf) else {
            continue;
        };
        let Ok(Record::Order(order)) = serde_json::from_str::<Record>(line.trim()) else {
            continue;
        };
        if position_id_from_order(&order.order.client_order_id.0) != Some(position_id) {
            continue;
        }
        out.orders
            .insert(order.order.client_order_id.0.clone(), *order);
    }
    out
}

struct Folded {
    broken: usize,
    order_ids: HashSet<String>,
}

fn fold_lines(replayed: &mut Replayed, text: &str) -> Folded {
    let mut broken = 0usize;
    let mut order_ids = HashSet::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Record>(line) {
            Ok(Record::Order(order)) => {
                order_ids.insert(order.order.client_order_id.0.clone());
                replayed
                    .orders
                    .insert(order.order.client_order_id.0.clone(), *order);
            }
            Ok(Record::Position(position)) => {
                replayed.positions.insert(position.id.clone(), *position);
            }
            Err(_) => broken += 1,
        }
    }
    Folded { broken, order_ids }
}

fn stamp_orders(
    replayed: &Replayed,
    order_at: &mut HashMap<String, DateTime<Utc>>,
    seen: &HashSet<String>,
    incremental: bool,
    now: DateTime<Utc>,
) {
    for id in seen {
        if incremental {
            order_at.insert(id.clone(), now);
            continue;
        }
        // 整份重放没有「刚才追加」的时刻。能对上仓位的，用仓位自己的时间，避免把旧订单一律当成今天。
        if let Some(position) = position_of_order(id, &replayed.positions) {
            order_at
                .entry(id.clone())
                .or_insert_with(|| position.closed_at.unwrap_or(position.opened_at));
        }
    }
}

/// 压实进快照的那一份。调用方手里的 [`Replayed`] 不动。
fn prune_snapshot(
    replayed: &Replayed,
    order_at: &HashMap<String, DateTime<Utc>>,
    now: DateTime<Utc>,
) -> (
    HashMap<String, OrderState>,
    HashMap<String, PairPosition>,
    HashMap<String, DateTime<Utc>>,
) {
    let today = beijing_date(now);
    let recent: HashSet<String> = recent_ended_ids(&replayed.positions).into_iter().collect();
    let positions = replayed
        .positions
        .iter()
        .filter(|(id, position)| keep_position(position, id, &recent, today))
        .map(|(id, position)| (id.clone(), position.clone()))
        .collect();
    let mut orders = HashMap::new();
    let mut kept_at = HashMap::new();
    for (id, order) in &replayed.orders {
        if !keep_order(id, order, order_at, &replayed.positions, today) {
            continue;
        }
        orders.insert(id.clone(), order.clone());
        if let Some(at) = order_at.get(id) {
            kept_at.insert(id.clone(), *at);
        }
    }
    (orders, positions, kept_at)
}

fn keep_position(
    position: &PairPosition,
    id: &str,
    recent_ended: &HashSet<String>,
    today: NaiveDate,
) -> bool {
    if position.status.has_exposure() || !is_ended(position) {
        return true;
    }
    if position.realized_source.is_none() {
        return true;
    }
    let at = position.closed_at.unwrap_or(position.opened_at);
    in_keep_window(at, today) || recent_ended.contains(id)
}

fn recent_ended_ids(positions: &HashMap<String, PairPosition>) -> Vec<String> {
    let mut ended: Vec<&PairPosition> = positions
        .values()
        .filter(|position| is_ended(position))
        .collect();
    ended.sort_by(|left, right| {
        right
            .opened_at
            .cmp(&left.opened_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    ended
        .into_iter()
        .take(KEEP_RECENT_ENDED)
        .map(|position| position.id.clone())
        .collect()
}

fn keep_order(
    id: &str,
    order: &OrderState,
    order_at: &HashMap<String, DateTime<Utc>>,
    positions: &HashMap<String, PairPosition>,
    today: NaiveDate,
) -> bool {
    if order.status.is_live() {
        return true;
    }
    if order_at
        .get(id)
        .is_some_and(|at| in_keep_window(*at, today))
    {
        return true;
    }
    let Some(position) = position_of_order(id, positions) else {
        return false;
    };
    if position.status.has_exposure() || position.realized_source.is_none() {
        return true;
    }
    let at = position.closed_at.unwrap_or(position.opened_at);
    is_ended(position) && in_keep_window(at, today)
}

fn is_ended(position: &PairPosition) -> bool {
    matches!(
        position.status,
        PositionStatus::Closed | PositionStatus::Unwound
    )
}

fn position_of_order<'a>(
    order_id: &str,
    positions: &'a HashMap<String, PairPosition>,
) -> Option<&'a PairPosition> {
    let id = position_id_from_order(order_id)?;
    positions.get(id)
}

/// `{仓位}-buy-0`、`{仓位}-sell-trim1`、`{仓位}-buy-exit2`。对不上就不是这笔仓位的单。
fn position_id_from_order(order_id: &str) -> Option<&str> {
    let (head, tail) = order_id.rsplit_once('-')?;
    let tail_ok = tail.chars().all(|ch| ch.is_ascii_digit())
        || tail.starts_with("trim")
        || tail.starts_with("exit");
    if !tail_ok {
        return None;
    }
    let (id, side) = head.rsplit_once('-')?;
    (side == "buy" || side == "sell").then_some(id)
}

fn in_keep_window(at: DateTime<Utc>, today: NaiveDate) -> bool {
    let day = beijing_date(at);
    let from = today
        .checked_sub_signed(chrono::Duration::days(KEEP_DAYS - 1))
        .unwrap_or(today);
    day >= from && day <= today
}

fn beijing_date(at: DateTime<Utc>) -> NaiveDate {
    at.with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).expect("UTC+8"))
        .date_naive()
}

fn snapshot_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".snapshot.json");
    PathBuf::from(name)
}

/// 快照必须落在台账现有长度之内，并且切在换行上。对不上就整份重放。
async fn load_snapshot(path: &Path, file_len: u64) -> Option<LedgerSnapshot> {
    let bytes = tokio::fs::read(snapshot_path(path)).await.ok()?;
    let text = String::from_utf8(bytes).ok()?;
    let snap: LedgerSnapshot = serde_json::from_str(text.trim()).ok()?;
    if snap.version != SNAPSHOT_VERSION || snap.through > file_len {
        return None;
    }
    if snap.through > 0 {
        let mut file = tokio::fs::File::open(path).await.ok()?;
        file.seek(SeekFrom::Start(snap.through - 1)).await.ok()?;
        let mut byte = [0u8; 1];
        if file.read_exact(&mut byte).await.is_err() || byte[0] != b'\n' {
            return None;
        }
    }
    Some(snap)
}

async fn write_snapshot(path: &Path, snapshot: &LedgerSnapshot) -> std::io::Result<()> {
    let dest = snapshot_path(path);
    let tmp = {
        let mut name = dest.as_os_str().to_owned();
        name.push(".tmp");
        PathBuf::from(name)
    };
    let mut payload = serde_json::to_vec(snapshot)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    payload.push(b'\n');
    if let Some(parent) = dest.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(&tmp, &payload).await?;
    tokio::fs::rename(&tmp, &dest).await?;
    Ok(())
}

/// 把当前整份台账收成快照。查询路径在新增部分够大时自己收；测试可以直接调用。
#[cfg(test)]
async fn refresh_snapshot(path: &Path) -> std::io::Result<()> {
    let (text, covered) = read_range(path, 0).await?;
    let mut replayed = Replayed::default();
    let folded = fold_lines(&mut replayed, &text);
    let now = Utc::now();
    let mut order_at = HashMap::new();
    stamp_orders(&replayed, &mut order_at, &folded.order_ids, false, now);
    let (orders, positions, order_at) = prune_snapshot(&replayed, &order_at, now);
    write_snapshot(
        path,
        &LedgerSnapshot {
            version: SNAPSHOT_VERSION,
            through: covered,
            broken: folded.broken,
            orders,
            positions,
            order_at,
        },
    )
    .await
}

/// 从 `start` 读到最后一个完整行。返回文本，以及这些完整行在文件里的结束位置。
async fn read_range(path: &Path, start: u64) -> std::io::Result<(String, u64)> {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((String::new(), 0));
        }
        Err(error) => return Err(error),
    };
    let len = file.metadata().await?.len();
    if start > len {
        return Ok((String::new(), start));
    }
    file.seek(SeekFrom::Start(start)).await?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).await?;
    let end_rel = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |at| at + 1);
    let text = String::from_utf8_lossy(&bytes[..end_rel]).into_owned();
    Ok((text, start + end_rel as u64))
}

/// 台账旁路文件：崩溃留下的半行被挪到这里。
fn torn_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".torn");
    PathBuf::from(name)
}

/// 读出台账里**写完整了**的部分（到最后一个换行为止）。文件不存在返回空串。
///
/// 末尾没有换行的那一段要么是另一个任务正在追加的一行（读到一半），要么是崩溃留下的半行
/// （下次 [`Ledger::open`] 会处理）：两种都不是已确认的记录，所以不解析、也不算坏行。
/// 按字节切分后再解码：半行可能正好断在一个多字节字符中间，整体 `read_to_string` 会因此
/// 报错，让同一时刻的另一个读者（看板刷新）连带读不了整份台账。
async fn read_complete_lines(path: &Path) -> std::io::Result<String> {
    Ok(read_range(path, 0).await?.0)
}

/// 文件末尾没有换行时，返回最后一行的起始字节和内容。已经以换行结束则是 `None`。
/// 从末尾往前分块找换行，不读整本台账。
async fn incomplete_tail(path: &Path) -> std::io::Result<Option<(u64, Vec<u8>)>> {
    let mut file = tokio::fs::File::open(path).await?;
    let len = file.metadata().await?.len();
    if len == 0 {
        return Ok(None);
    }
    file.seek(SeekFrom::End(-1)).await?;
    let mut last = [0u8; 1];
    file.read_exact(&mut last).await?;
    if last[0] == b'\n' {
        return Ok(None);
    }
    const CHUNK: u64 = 64 * 1024;
    let mut end = len;
    loop {
        let take = CHUNK.min(end);
        let at = end - take;
        file.seek(SeekFrom::Start(at)).await?;
        let mut buf = vec![0u8; take as usize];
        file.read_exact(&mut buf).await?;
        if let Some(rel) = buf.iter().rposition(|byte| *byte == b'\n') {
            let start = at + rel as u64 + 1;
            let mut tail = buf[rel + 1..].to_vec();
            if end < len {
                file.seek(SeekFrom::Start(end)).await?;
                file.read_to_end(&mut tail).await?;
            }
            return Ok(Some((start, tail)));
        }
        if at == 0 {
            file.seek(SeekFrom::Start(0)).await?;
            let mut all = Vec::new();
            file.read_to_end(&mut all).await?;
            return Ok(Some((0, all)));
        }
        end = at;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        ClientOrderId, NewOrder, OrderState, PairPosition, PositionStatus, Strategy, TaskRules,
    };
    use arb_core::{Side, Symbol, Venue};
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn order(id: &str, notional: Decimal) -> OrderState {
        OrderState::new(NewOrder {
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId(id.into()),
            venue: Venue::Binance,
            symbol: Symbol::perp("BTC", "USDT"),
            side: Side::Buy,
            notional_usdt: notional,
            limit_price: None,
            quantity: None,
            reduce_only: false,
            leverage: None,
        })
    }

    fn position(id: &str, status: PositionStatus) -> PairPosition {
        PairPosition {
            margin_mode: crate::MarginMode::Isolated,
            id: id.into(),
            symbol: Symbol::perp("BTC", "USDT"),
            strategy: Strategy::Funding,
            long: None,
            short: None,
            entry_basis_pct: dec!(0.1),
            expected_round_trip_cost: dec!(0.002),
            status,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: None,
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned_long: None,
            planned_short: None,
            settled_legs: Vec::new(),
            funding_legs: Vec::new(),
            origin: None,
        }
    }

    #[test]
    fn a_position_line_written_before_leverage_and_rules_still_replays() {
        // 加入杠杆 / 规则 / 保证金字段之前的台账行形状。
        let line = r#"{"kind":"position","id":"paper-1","symbol":"BTC/USDT","strategy":"funding","long":{"venue":"binance","side":"buy","notional_usdt":"1000","average_price":"100","fee_usdt":"0.5","client_order_id":"paper-1-buy-0"},"short":null,"entry_basis_pct":"0.1","expected_round_trip_cost":"0.002","status":"opening","opened_at":"2026-09-22T10:00:00Z","closed_at":null,"note":null}"#;
        let Record::Position(position) = serde_json::from_str::<Record>(line).unwrap() else {
            panic!("应当解析成仓位");
        };
        assert_eq!(position.leverage, None);
        assert!(position.rules.is_empty(), "旧仓位没有任何规则");
        assert_eq!(position.trims, 0);
        assert_eq!(position.long.unwrap().margin_usdt, None);
    }

    #[tokio::test]
    async fn replay_keeps_the_latest_state_per_key() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Ledger::open(&path).await.unwrap();

        ledger
            .append(&Record::Order(Box::new(order("o1", dec!(100)))))
            .await
            .unwrap();
        let mut progressed = order("o1", dec!(100));
        progressed.status = crate::types::OrderStatus::Filled;
        progressed.filled_usdt = dec!(100);
        ledger
            .append(&Record::Order(Box::new(progressed)))
            .await
            .unwrap();

        let (replayed, broken) = ledger.replay().await.unwrap();
        assert_eq!(broken, 0);
        assert_eq!(replayed.orders.len(), 1, "同一个 key 只保留最新一条");
        assert_eq!(
            replayed.orders["o1"].status,
            crate::types::OrderStatus::Filled
        );

        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// 崩溃留下的半行：重启时挪到旁路文件、从台账截掉，而不是封口成永久的坏行
    /// （坏行会让对账拒绝一切开仓与规则）。追加的后续记录不受牵连。
    #[tokio::test]
    async fn a_torn_tail_is_quarantined_not_sealed_into_a_permanent_bad_line() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-torn-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let _ = tokio::fs::remove_file(torn_path(&path)).await;
        let ledger = Ledger::open(&path).await.unwrap();
        ledger
            .append(&Record::Order(Box::new(order("o1", dec!(100)))))
            .await
            .unwrap();
        drop(ledger);

        // 半行写入（崩溃现场），故意断在多字节字符中间：整体 `read_to_string` 会因此报错。
        let torn = "{\"kind\":\"order\",\"note\":\"第一腿".as_bytes();
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        file.write_all(&torn[..torn.len() - 1]).await.unwrap();
        drop(file);

        // 重启前别的读者（看板刷新）读到它：不报错、不当坏行。
        let (seen, broken) = replay_file(&path).await.unwrap();
        assert_eq!((seen.orders.len(), broken), (1, 0));

        let ledger = Ledger::open(&path).await.unwrap();
        ledger
            .append(&Record::Order(Box::new(order("o2", dec!(200)))))
            .await
            .unwrap();
        let (replayed, broken) = ledger.replay().await.unwrap();
        assert_eq!(broken, 0, "半行不能留下坏行：对账见到坏行会拒绝一切操作");
        assert_eq!(replayed.orders.len(), 2);
        assert!(replayed.orders.contains_key("o2"));
        let kept = tokio::fs::read(torn_path(&path)).await.unwrap();
        assert!(kept.starts_with(&torn[..torn.len() - 1]), "半行要留证");

        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_file(torn_path(&path)).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// 只缺换行的完整记录是一条真记录：保留，补上换行。
    #[tokio::test]
    async fn a_complete_record_missing_only_its_newline_is_kept() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-nl-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let line = serde_json::to_string(&Record::Order(Box::new(order("o1", dec!(100))))).unwrap();
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(&path, line).await.unwrap();

        let ledger = Ledger::open(&path).await.unwrap();
        ledger
            .append(&Record::Order(Box::new(order("o2", dec!(200)))))
            .await
            .unwrap();
        let (replayed, broken) = ledger.replay().await.unwrap();
        assert_eq!((replayed.orders.len(), broken), (2, 0));

        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// 文件中间的坏行（不是崩溃的尾巴）仍然只被计数：对账据此拒绝操作，由人核对。
    #[tokio::test]
    async fn a_corrupt_line_in_the_middle_is_counted_and_does_not_destroy_the_history() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-bad-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Ledger::open(&path).await.unwrap();
        ledger
            .append(&Record::Order(Box::new(order("o1", dec!(100)))))
            .await
            .unwrap();
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        file.write_all(b"not json at all\n").await.unwrap();
        drop(file);
        ledger
            .append(&Record::Order(Box::new(order("o2", dec!(200)))))
            .await
            .unwrap();

        let (replayed, broken) = ledger.replay().await.unwrap();
        assert_eq!(broken, 1, "坏行被计数");
        assert_eq!(replayed.orders.len(), 2, "其余记录照常重放");

        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn only_positions_with_exposure_are_reported() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-exp-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Ledger::open(&path).await.unwrap();
        ledger
            .append(&Record::Position(Box::new(position(
                "open",
                PositionStatus::Open,
            ))))
            .await
            .unwrap();
        ledger
            .append(&Record::Position(Box::new(position(
                "closed",
                PositionStatus::Closed,
            ))))
            .await
            .unwrap();
        ledger
            .append(&Record::Position(Box::new(position(
                "naked",
                PositionStatus::Unwinding,
            ))))
            .await
            .unwrap();

        let (replayed, _) = ledger.replay().await.unwrap();
        let exposed: Vec<&str> = replayed.exposed().iter().map(|p| p.id.as_str()).collect();
        assert!(exposed.contains(&"open"));
        assert!(exposed.contains(&"naked"), "回滚中的仓位仍然有敞口");
        assert!(!exposed.contains(&"closed"));

        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// 快照记下最新状态。之后即使前缀被弄坏，查询仍用快照加上其后的新行；
    /// 一条仓位的全过程仍从原文件翻，所以快照不能代替历史。
    #[tokio::test]
    async fn snapshot_plus_tail_matches_a_full_replay_and_ignores_a_corrupt_prefix() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-snap-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let ledger = Ledger::open(&path).await.unwrap();
        ledger
            .append(&Record::Order(Box::new(order("p1-buy-0", dec!(100)))))
            .await
            .unwrap();
        let mut filled = order("p1-buy-0", dec!(100));
        filled.status = crate::types::OrderStatus::Filled;
        filled.filled_usdt = dec!(100);
        ledger
            .append(&Record::Order(Box::new(filled)))
            .await
            .unwrap();
        ledger
            .append(&Record::Position(Box::new(position(
                "p1",
                PositionStatus::Open,
            ))))
            .await
            .unwrap();
        drop(ledger);
        refresh_snapshot(&path).await.unwrap();

        let history_before = {
            let ledger = Ledger::open(&path).await.unwrap();
            ledger.position_history("p1").await.unwrap()
        };
        assert_eq!(history_before.len(), 1);

        let ledger = Ledger::open(&path).await.unwrap();
        ledger
            .append(&Record::Order(Box::new(order("o2", dec!(50)))))
            .await
            .unwrap();
        ledger
            .append(&Record::Position(Box::new(position(
                "p1",
                PositionStatus::Closed,
            ))))
            .await
            .unwrap();
        drop(ledger);

        let through = {
            let bytes = tokio::fs::read(snapshot_path(&path)).await.unwrap();
            let text = std::str::from_utf8(&bytes).unwrap().trim();
            let snap: LedgerSnapshot = serde_json::from_str(text).unwrap();
            snap.through
        };
        let mut raw = tokio::fs::read(&path).await.unwrap();
        for byte in &mut raw[..through as usize] {
            if *byte != b'\n' {
                *byte = b'x';
            }
        }
        tokio::fs::write(&path, &raw).await.unwrap();

        let (replayed, broken) = replay_file(&path).await.unwrap();
        assert_eq!(broken, 0, "快照已经盖住的前缀不能再被当成坏行");
        assert_eq!(
            replayed.orders["p1-buy-0"].status,
            crate::types::OrderStatus::Filled
        );
        assert!(replayed.orders.contains_key("o2"));
        assert_eq!(replayed.positions["p1"].status, PositionStatus::Closed);

        let ahead = tokio::fs::read(snapshot_path(&path)).await.unwrap();
        let text = std::str::from_utf8(&ahead).unwrap().trim();
        let mut snap: serde_json::Value = serde_json::from_str(text).unwrap();
        snap["through"] = serde_json::json!(raw.len() as u64 + 10);
        let ahead = serde_json::to_vec(&snap).unwrap();
        tokio::fs::write(snapshot_path(&path), ahead).await.unwrap();
        let (fallback, broken) = replay_file(&path).await.unwrap();
        assert!(broken > 0, "快照对不上时退回整份重放，前缀里的坏行要被数到");
        assert!(fallback.orders.contains_key("o2"));
        assert_eq!(fallback.positions["p1"].status, PositionStatus::Closed);

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// 旧的已结算仓位和它名下超过 7 日的订单不进快照；未平仓、近 7 日的订单、仍挂着的单要留。
    #[tokio::test]
    async fn snapshot_keeps_recent_orders_and_drops_aged_out_history() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-keep-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let ledger = Ledger::open(&path).await.unwrap();
        let old_at = Utc::now() - chrono::Duration::days(40);
        let mut old = position("old", PositionStatus::Closed);
        old.opened_at = old_at;
        old.closed_at = Some(old_at);
        old.realized_source = Some(crate::types::RealizedSource::Executor);
        let mut aged = order("old-buy-0", dec!(10));
        aged.status = crate::types::OrderStatus::Filled;
        aged.filled_usdt = dec!(10);
        let mut still_open = position("open", PositionStatus::Open);
        still_open.opened_at = old_at;
        let mut open_order = order("open-buy-0", dec!(20));
        open_order.status = crate::types::OrderStatus::Filled;
        open_order.filled_usdt = dec!(20);
        let mut live = order("live-1", dec!(5));
        live.status = crate::types::OrderStatus::Open;
        let recent = position("recent", PositionStatus::Closed);
        let mut recent_order = order("recent-sell-0", dec!(30));
        recent_order.status = crate::types::OrderStatus::Filled;
        recent_order.filled_usdt = dec!(30);
        for record in [
            Record::Position(Box::new(old)),
            Record::Order(Box::new(aged)),
            Record::Position(Box::new(still_open)),
            Record::Order(Box::new(open_order)),
            Record::Order(Box::new(live)),
            Record::Position(Box::new(recent.clone())),
            Record::Order(Box::new(recent_order)),
        ] {
            ledger.append(&record).await.unwrap();
        }
        drop(ledger);
        refresh_snapshot(&path).await.unwrap();

        let (replayed, _) = replay_file(&path).await.unwrap();
        // 结束的笔还不到 64，这只旧仓位仍留着；它的订单已经超过 7 日，不留。
        assert!(replayed.positions.contains_key("old"));
        assert!(!replayed.orders.contains_key("old-buy-0"));
        assert_eq!(replayed.positions["open"].status, PositionStatus::Open);
        assert!(replayed.orders.contains_key("open-buy-0"));
        assert!(replayed.orders.contains_key("live-1"));
        assert_eq!(replayed.positions["recent"].status, PositionStatus::Closed);
        assert!(replayed.orders.contains_key("recent-sell-0"));

        let mut older = Vec::new();
        for index in 0..70 {
            let mut ended = position(&format!("aged-{index:02}"), PositionStatus::Closed);
            ended.opened_at = Utc::now()
                - chrono::Duration::days(10)
                - chrono::Duration::seconds(i64::from(index));
            ended.closed_at = Some(ended.opened_at);
            ended.realized_source = Some(crate::types::RealizedSource::Executor);
            older.push(ended);
        }
        let ledger = Ledger::open(&path).await.unwrap();
        for ended in &older {
            ledger
                .append(&Record::Position(Box::new(ended.clone())))
                .await
                .unwrap();
        }
        drop(ledger);
        refresh_snapshot(&path).await.unwrap();
        let (replayed, _) = replay_file(&path).await.unwrap();
        let kept_aged = replayed
            .positions
            .keys()
            .filter(|id| id.starts_with("aged-"))
            .count();
        // 近 7 日的 recent 占掉最近 64 笔里的一个名额，其余名额给 10 日前的那一批。
        assert_eq!(kept_aged, KEEP_RECENT_ENDED - 1);
        assert!(replayed.positions.contains_key("aged-00"));
        assert!(!replayed.positions.contains_key("aged-69"));
        assert!(!replayed.positions.contains_key("old"));
        assert!(replayed.positions.contains_key("open"));
        assert!(replayed.orders.contains_key("open-buy-0"));
        assert!(replayed.orders.contains_key("recent-sell-0"));

        tokio::fs::remove_dir_all(&dir).await.ok();
    }
}
