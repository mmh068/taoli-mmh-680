//! 累计已实现盈亏的滚动存档。
//!
//! 每次把整本台账加一遍也能得到累计值，但台账会越来越长，而且删掉旧记录后就加不回来了。
//! 这里在「盈亏真正结算清楚」或「资金费事后更正」时更新一份独立文件：
//! `{台账路径}.cumulative-pnl.json`。近 7 日和今日另见 `{台账路径}.window-pnl.json`。
//!
//! - 首次没有文件时，按当前台账引导一次；
//! - 之后只做增量：结束一单就记一单，新币种自动加一行；
//! - 同一仓位 id 只计入一次，资金费更正只加减差额。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::ledger::{Ledger, Replayed};
use crate::types::{OrderStatus, PairPosition, PositionStatus};

const VERSION: u32 = 3;

/// 单个币种的滚动合计。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SymbolRollup {
    pub volume_usdt: Decimal,
    pub opens: u64,
    pub net_usdt: Decimal,
    pub fee_usdt: Decimal,
    pub funding_usdt: Decimal,
    /// 已计入的回滚笔数。旧存档没有，按 0。
    #[serde(default)]
    pub unwound: u64,
    #[serde(default)]
    pub unwound_usdt: Decimal,
}

/// 单个成交交易所的滚动合计。字段与币种行相同。
///
/// 交易量、手续费按台账订单的成交场所分。有分腿盈亏、分腿资金费就用记下的数。
/// 没有分腿时，价格盈亏不倒推；资金费只有一家有成交才归到这家，多家不拆。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VenueRollup {
    pub volume_usdt: Decimal,
    pub opens: u64,
    pub net_usdt: Decimal,
    pub fee_usdt: Decimal,
    pub funding_usdt: Decimal,
    #[serde(default)]
    pub unwound: u64,
    #[serde(default)]
    pub unwound_usdt: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CumulativePnl {
    pub version: u32,
    /// 已计入的净额合计（价格 − 手续费 + 资金费）。
    pub net_usdt: Decimal,
    #[serde(default)]
    pub fee_usdt: Decimal,
    #[serde(default)]
    pub funding_usdt: Decimal,
    #[serde(default)]
    pub volume_usdt: Decimal,
    /// 已计入的仓位数。
    pub settled: u64,
    /// 已经加过累计的仓位 id，避免重复。
    pub included: BTreeSet<String>,
    /// 这些 id 的结束时间。用来裁掉过旧的 id，同时不把同一笔再加一次。
    #[serde(default)]
    pub included_at: BTreeMap<String, DateTime<Utc>>,
    /// 结束时间早于它的已结算仓位都已经在合计里，不必再留 id。
    #[serde(default)]
    pub included_through: Option<DateTime<Utc>>,
    /// 仓位 id → 币种，资金费更正时找到对应行。
    #[serde(default)]
    pub symbol_of: BTreeMap<String, String>,
    /// 按币种滚动合计。
    #[serde(default)]
    pub symbols: BTreeMap<String, SymbolRollup>,
    /// 按成交交易所滚动合计。
    #[serde(default)]
    pub venues: BTreeMap<String, VenueRollup>,
    /// 手动信号已结算净额。已包含在 `net_usdt` 里，只是单独标注。
    #[serde(default)]
    pub manual_net_usdt: Decimal,
    /// 上面这项包含的笔数。
    #[serde(default)]
    pub manual_settled: u64,
    /// 已经计入手动合计的仓位 id。旧存档升级时用来避免重复。
    #[serde(default)]
    pub manual_ids: BTreeSet<String>,
    #[serde(default)]
    pub unwound: u64,
    #[serde(default)]
    pub unwound_usdt: Decimal,
    /// 回滚笔数已经按当时的台账算清。之后只随新结算增减，不再整表重算。
    #[serde(default)]
    unwound_sealed: bool,
    pub updated_at: DateTime<Utc>,
}

impl Default for CumulativePnl {
    fn default() -> Self {
        Self {
            version: VERSION,
            net_usdt: Decimal::ZERO,
            fee_usdt: Decimal::ZERO,
            funding_usdt: Decimal::ZERO,
            volume_usdt: Decimal::ZERO,
            settled: 0,
            included: BTreeSet::new(),
            included_at: BTreeMap::new(),
            included_through: None,
            symbol_of: BTreeMap::new(),
            symbols: BTreeMap::new(),
            venues: BTreeMap::new(),
            manual_net_usdt: Decimal::ZERO,
            manual_settled: 0,
            manual_ids: BTreeSet::new(),
            unwound: 0,
            unwound_usdt: Decimal::ZERO,
            unwound_sealed: false,
            updated_at: Utc::now(),
        }
    }
}

impl CumulativePnl {
    pub fn path_for(ledger: &Path) -> PathBuf {
        let mut name = ledger.as_os_str().to_owned();
        name.push(".cumulative-pnl.json");
        PathBuf::from(name)
    }

    pub async fn load(path: &Path) -> std::io::Result<Self> {
        let bytes = tokio::fs::read(path).await?;
        Self::from_bytes(&bytes)
    }

    pub async fn save(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp = {
            let mut tmp = path.as_os_str().to_owned();
            tmp.push(".tmp");
            PathBuf::from(tmp)
        };
        let payload = serde_json::to_vec_pretty(self)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        {
            let mut file = tokio::fs::File::create(&tmp).await?;
            file.write_all(&payload).await?;
            file.write_all(b"\n").await?;
            file.sync_data().await?;
        }
        tokio::fs::rename(&tmp, path).await?;
        Ok(())
    }

    /// 读已有存档；没有就按整本台账引导一次并落盘。不用快照回放。
    pub async fn load_or_bootstrap(ledger: &Ledger) -> std::io::Result<Self> {
        let path = Self::path_for(ledger.path());
        if path.exists()
            && let Ok(value) = Self::load_sync(&path)
        {
            return Ok(value);
        }
        let replayed = crate::ledger::replay_full_sync(ledger.path())?;
        let value = Self::bootstrap_from_replay(&replayed);
        let _ = value.save_sync(&path);
        Ok(value)
    }

    pub fn load_sync(path: &Path) -> std::io::Result<Self> {
        let bytes = std::fs::read(path)?;
        Self::from_bytes(&bytes)
    }

    fn from_bytes(bytes: &[u8]) -> std::io::Result<Self> {
        let mut value: Self = serde_json::from_slice(bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        value.net_usdt = value.net_usdt.round_dp(8);
        value.fee_usdt = value.fee_usdt.round_dp(8);
        value.funding_usdt = value.funding_usdt.round_dp(8);
        value.volume_usdt = value.volume_usdt.round_dp(8);
        Ok(value)
    }

    pub fn save_sync(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = {
            let mut tmp = path.as_os_str().to_owned();
            tmp.push(".tmp");
            PathBuf::from(tmp)
        };
        let payload = serde_json::to_vec_pretty(self)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        std::fs::write(&tmp, payload)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// 有文件就读，打开页面不重算。没有文件才按整本台账建一次。
    pub fn load_or_bootstrap_from_replay(ledger_path: &Path, replayed: &Replayed) -> Self {
        let path = Self::path_for(ledger_path);
        if path.exists()
            && let Ok(value) = Self::load_sync(&path)
        {
            return value;
        }
        let Ok(source) = crate::ledger::replay_full_sync(ledger_path) else {
            // 整本没读成就不要落盘。落下快照并打上已补标记，以后就补不回来。
            return Self::bootstrap_from_replay(replayed);
        };
        let value = Self::bootstrap_from_replay(&source);
        let _ = value.save_sync(&path);
        value
    }

    pub fn bootstrap_from_replay(replayed: &Replayed) -> Self {
        let mut value = Self::default();
        let mut ended: Vec<&PairPosition> = replayed
            .positions
            .values()
            .filter(|position| ended_position(position))
            .collect();
        ended.sort_by(|a, b| a.id.cmp(&b.id));
        for position in ended {
            if settled_net(position).is_some() {
                value.record_from_position(
                    position,
                    replayed,
                    filled_volume(replayed, &position.id),
                );
            }
        }
        value.updated_at = Utc::now();
        value
    }

    pub fn bootstrap_from_positions<'a>(positions: impl Iterator<Item = &'a PairPosition>) -> Self {
        let mut value = Self::default();
        for position in positions {
            if settled_net(position).is_some() {
                value.record_from_position(position, &Replayed::default(), Decimal::ZERO);
            }
        }
        value.updated_at = Utc::now();
        value
    }

    fn mark_manual(&mut self, position_id: &str, net: Decimal) {
        if !self.manual_ids.insert(position_id.to_string()) {
            return;
        }
        self.manual_settled += 1;
        self.manual_net_usdt = (self.manual_net_usdt + net).round_dp(8);
    }

    /// 首次计入一笔已结算仓位。已计入过则返回 false。
    pub fn record_from_position(
        &mut self,
        position: &PairPosition,
        replayed: &Replayed,
        volume_usdt: Decimal,
    ) -> bool {
        let Some(net) = settled_net(position) else {
            return false;
        };
        if self.already_recorded(position) {
            return false;
        }
        let recorded = self.record_parts(
            &position.id,
            &position.symbol.to_string(),
            net,
            position.realized_fee_usdt,
            position.realized_funding_usdt.unwrap_or_default(),
            volume_usdt,
        );
        if recorded {
            self.add_venue(venue_parts(replayed, position));
            self.included_at.insert(
                position.id.clone(),
                position.closed_at.unwrap_or(position.opened_at),
            );
            if position.status == PositionStatus::Unwound {
                self.unwound += 1;
                self.unwound_usdt = (self.unwound_usdt + volume_usdt).round_dp(8);
                if let Some(row) = self.symbols.get_mut(&position.symbol.to_string()) {
                    row.unwound += 1;
                    row.unwound_usdt = (row.unwound_usdt + volume_usdt).round_dp(8);
                }
            }
        }
        if recorded && position.origin.as_deref() == Some("manual") {
            self.mark_manual(&position.id, net);
        }
        recorded
    }

    pub fn record_parts(
        &mut self,
        position_id: &str,
        symbol: &str,
        net: Decimal,
        fee: Decimal,
        funding: Decimal,
        volume: Decimal,
    ) -> bool {
        if !self.included.insert(position_id.to_string()) {
            return false;
        }
        self.settled += 1;
        self.net_usdt = (self.net_usdt + net).round_dp(8);
        self.fee_usdt = (self.fee_usdt + fee).round_dp(8);
        self.funding_usdt = (self.funding_usdt + funding).round_dp(8);
        self.volume_usdt = (self.volume_usdt + volume).round_dp(8);
        self.symbol_of
            .insert(position_id.to_string(), symbol.to_string());
        let row = self.symbols.entry(symbol.to_string()).or_default();
        row.opens += 1;
        row.net_usdt = (row.net_usdt + net).round_dp(8);
        row.fee_usdt = (row.fee_usdt + fee).round_dp(8);
        row.funding_usdt = (row.funding_usdt + funding).round_dp(8);
        row.volume_usdt = (row.volume_usdt + volume).round_dp(8);
        self.updated_at = Utc::now();
        true
    }

    /// 兼容旧调用：只加总净额，币种记为 unknown。
    pub fn record_settlement(&mut self, position_id: &str, net: Decimal) -> bool {
        self.record_parts(
            position_id,
            "UNKNOWN",
            net,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        )
    }

    /// 资金费事后更正：只加减差额。
    pub fn apply_funding_delta(
        &mut self,
        position_id: &str,
        previous: Option<Decimal>,
        actual: Decimal,
    ) -> bool {
        self.apply_funding_delta_venues(position_id, previous, actual, &[])
    }

    /// 资金费更正同时改交易所行。差额来自入账时记下的分腿，没有分腿就不改交易所行。
    pub fn apply_funding_delta_venues(
        &mut self,
        position_id: &str,
        previous: Option<Decimal>,
        actual: Decimal,
        venue_deltas: &[(String, Decimal)],
    ) -> bool {
        if !self.apply_funding_delta_totals(position_id, previous, actual) {
            return false;
        }
        for (venue, delta) in venue_deltas {
            if *delta == Decimal::ZERO {
                continue;
            }
            let row = self.venues.entry(venue.clone()).or_default();
            row.funding_usdt = (row.funding_usdt + delta).round_dp(8);
            row.net_usdt = (row.net_usdt + delta).round_dp(8);
        }
        true
    }

    fn apply_funding_delta_totals(
        &mut self,
        position_id: &str,
        previous: Option<Decimal>,
        actual: Decimal,
    ) -> bool {
        if !self.included.contains(position_id) {
            return false;
        }
        let old = previous.unwrap_or(Decimal::ZERO);
        let delta = actual - old;
        self.net_usdt = (self.net_usdt + delta).round_dp(8);
        self.funding_usdt = (self.funding_usdt + delta).round_dp(8);
        if let Some(symbol) = self.symbol_of.get(position_id).cloned() {
            if let Some(row) = self.symbols.get_mut(&symbol) {
                row.net_usdt = (row.net_usdt + delta).round_dp(8);
                row.funding_usdt = (row.funding_usdt + delta).round_dp(8);
            }
        }
        if self.manual_ids.contains(position_id) {
            self.manual_net_usdt = (self.manual_net_usdt + delta).round_dp(8);
        }
        self.updated_at = Utc::now();
        true
    }

    fn add_venue(&mut self, parts: Vec<VenuePart>) {
        for part in parts {
            let row = self.venues.entry(part.venue).or_default();
            row.volume_usdt = (row.volume_usdt + part.volume_usdt).round_dp(8);
            row.opens += part.opens;
            row.net_usdt = (row.net_usdt + part.net_usdt).round_dp(8);
            row.fee_usdt = (row.fee_usdt + part.fee_usdt).round_dp(8);
            row.funding_usdt = (row.funding_usdt + part.funding_usdt).round_dp(8);
            row.unwound += part.unwound;
            row.unwound_usdt = (row.unwound_usdt + part.unwound_usdt).round_dp(8);
        }
    }
}

#[derive(Default)]
pub(crate) struct VenuePart {
    pub venue: String,
    pub volume_usdt: Decimal,
    pub opens: u64,
    pub net_usdt: Decimal,
    pub fee_usdt: Decimal,
    pub unwound: u64,
    pub unwound_usdt: Decimal,
    pub funding_usdt: Decimal,
}

/// 一笔仓位拆到各成交交易所。
///
/// 交易量按成交单的场所分。有分腿时，盈亏和手续费只用分腿，不再把成交单手续费加一遍。
/// 没有分腿时，手续费用成交单。没有分腿资金费时，只有一家有成交才归到这家，多家不拆。
/// 开仓和回滚按仓位计，同一家多条腿只算一笔。
pub(crate) fn venue_parts(replayed: &Replayed, position: &PairPosition) -> Vec<VenuePart> {
    let mut by_venue: BTreeMap<String, VenuePart> = BTreeMap::new();
    let mut from_fill = BTreeSet::new();
    let prefix = format!("{}-", position.id);
    let booked = !position.settled_legs.is_empty();
    for order in replayed.orders.values() {
        if !order.order.client_order_id.0.starts_with(&prefix)
            || (order.filled_usdt <= Decimal::ZERO && order.status != OrderStatus::Filled)
        {
            continue;
        }
        let venue = order.order.venue.as_str().to_string();
        from_fill.insert(venue.clone());
        let row = by_venue.entry(venue).or_default();
        row.volume_usdt += order.filled_usdt;
        if !booked {
            row.fee_usdt += order.fee_usdt;
            row.net_usdt -= order.fee_usdt;
        }
    }
    for leg in &position.settled_legs {
        let venue = leg.venue.as_str().to_string();
        from_fill.insert(venue.clone());
        let row = by_venue.entry(venue).or_default();
        row.net_usdt += leg.price_pnl_usdt;
        row.fee_usdt += leg.fee_usdt;
        row.net_usdt -= leg.fee_usdt;
    }
    if position.funding_legs.is_empty() {
        if let Some(funding) = position.realized_funding_usdt.filter(|_| from_fill.len() == 1) {
            if let Some(venue) = from_fill.iter().next() {
                let row = by_venue.entry(venue.clone()).or_default();
                row.funding_usdt += funding;
                row.net_usdt += funding;
            }
        }
    }
    for leg in &position.funding_legs {
        let row = by_venue.entry(leg.venue.as_str().to_string()).or_default();
        row.funding_usdt += leg.usdt;
        row.net_usdt += leg.usdt;
    }
    let settled = settled_net(position).is_some();
    let unwound = position.status == PositionStatus::Unwound;
    let mut parts = Vec::new();
    for (venue, mut row) in by_venue {
        if settled && from_fill.contains(&venue) {
            row.opens = 1;
        }
        if unwound && from_fill.contains(&venue) {
            row.unwound = 1;
            row.unwound_usdt = row.volume_usdt;
        }
        row.volume_usdt = row.volume_usdt.round_dp(8);
        row.net_usdt = row.net_usdt.round_dp(8);
        row.fee_usdt = row.fee_usdt.round_dp(8);
        row.funding_usdt = row.funding_usdt.round_dp(8);
        row.unwound_usdt = row.unwound_usdt.round_dp(8);
        if row.volume_usdt == Decimal::ZERO
            && row.opens == 0
            && row.net_usdt == Decimal::ZERO
            && row.fee_usdt == Decimal::ZERO
            && row.funding_usdt == Decimal::ZERO
            && row.unwound == 0
        {
            continue;
        }
        row.venue = venue;
        parts.push(row);
    }
    parts
}

/// 快照丢掉旧仓位之前调用：把这次回放里还没计入的已结算仓位补进累计。
///
/// 不读整本。开仓单还在这次回放里就用它；已经被裁掉的，只按这个仓位号扫订单。
pub fn archive_before_snapshot_prune(ledger_path: &Path, replayed: &crate::ledger::Replayed) {
    let path = CumulativePnl::path_for(ledger_path);
    let mut store = match CumulativePnl::load_sync(&path) {
        Ok(value) => value,
        Err(_) => {
            let Ok(full) = crate::ledger::replay_full_sync(ledger_path) else {
                return;
            };
            let value = CumulativePnl::bootstrap_from_replay(&full);
            let _ = value.save_sync(&path);
            value
        }
    };
    let mut changed = false;
    let mut ended: Vec<&PairPosition> = replayed
        .positions
        .values()
        .filter(|position| ended_position(position) && settled_net(position).is_some())
        .collect();
    ended.sort_by(|left, right| left.id.cmp(&right.id));
    for position in ended {
        if store.already_recorded(position) {
            continue;
        }
        let fills =
            crate::ledger::orders_for_position(ledger_path, Some(replayed), &position.id);
        if store.record_from_position(position, &fills, filled_volume(&fills, &position.id)) {
            changed = true;
        }
    }
    changed |= store.note_times(replayed);
    changed |= store.seal_unwound_if_complete(replayed);
    changed |= store.prune_tracked_ids(Utc::now());
    if changed {
        let _ = store.save_sync(&path);
    }
}

const MAX_RECENT_IDS: usize = 1024;

impl CumulativePnl {
    /// 这笔已经在合计里（id 还在，或结束时间已经被水位盖住）。
    pub fn already_recorded(&self, position: &PairPosition) -> bool {
        if self.included.contains(&position.id) {
            return true;
        }
        let Some(through) = self.included_through else {
            return false;
        };
        position.closed_at.unwrap_or(position.opened_at) < through
    }

    fn note_times(&mut self, replayed: &crate::ledger::Replayed) -> bool {
        let mut changed = false;
        for id in self.included.clone() {
            if self.included_at.contains_key(&id) {
                continue;
            }
            let Some(position) = replayed.positions.get(&id) else {
                continue;
            };
            self.included_at
                .insert(id, position.closed_at.unwrap_or(position.opened_at));
            changed = true;
        }
        changed
    }

    fn seal_unwound_if_complete(&mut self, replayed: &crate::ledger::Replayed) -> bool {
        if self.unwound_sealed {
            return false;
        }
        if !self
            .included
            .iter()
            .all(|id| replayed.positions.contains_key(id))
        {
            return false;
        }
        self.recompute_unwound(replayed);
        self.unwound_sealed = true;
        true
    }

    fn recompute_unwound(&mut self, replayed: &crate::ledger::Replayed) {
        self.unwound = 0;
        self.unwound_usdt = Decimal::ZERO;
        for row in self.symbols.values_mut() {
            row.unwound = 0;
            row.unwound_usdt = Decimal::ZERO;
        }
        let mut ids: Vec<&PairPosition> = replayed
            .positions
            .values()
            .filter(|position| {
                position.status == PositionStatus::Unwound && self.included.contains(&position.id)
            })
            .collect();
        ids.sort_by(|left, right| left.id.cmp(&right.id));
        for position in ids {
            let volume = filled_volume(replayed, &position.id);
            self.unwound += 1;
            self.unwound_usdt = (self.unwound_usdt + volume).round_dp(8);
            if let Some(row) = self.symbols.get_mut(&position.symbol.to_string()) {
                row.unwound += 1;
                row.unwound_usdt = (row.unwound_usdt + volume).round_dp(8);
            }
        }
    }

    /// id 超过 1024 时，近 7 个北京日和最近 1024 笔留下，更早的并进水位。合计数字不改。
    fn prune_tracked_ids(&mut self, now: DateTime<Utc>) -> bool {
        if self.included.len() <= MAX_RECENT_IDS {
            return false;
        }
        let today = beijing_day(now);
        let from = today
            .checked_sub_signed(chrono::Duration::days(6))
            .unwrap_or(today);
        let mut timed = Vec::new();
        let mut keep = BTreeSet::new();
        for id in &self.included {
            match self.included_at.get(id) {
                Some(at) => timed.push((*at, id.clone())),
                None => {
                    keep.insert(id.clone());
                }
            }
        }
        timed.sort();
        for (at, id) in &timed {
            let day = beijing_day(*at);
            if day >= from && day <= today {
                keep.insert(id.clone());
            }
        }
        for (_, id) in timed.iter().rev().take(MAX_RECENT_IDS) {
            keep.insert(id.clone());
        }
        let Some(max_dropped) = timed
            .iter()
            .filter(|(_, id)| !keep.contains(id))
            .map(|(at, _)| *at)
            .max()
        else {
            return false;
        };
        for (at, id) in &timed {
            if *at == max_dropped {
                keep.insert(id.clone());
            }
        }
        if timed.iter().all(|(_, id)| keep.contains(id)) && keep.len() >= self.included.len() {
            return false;
        }
        self.included_through = Some(match self.included_through {
            Some(existing) if existing >= max_dropped => existing,
            _ => max_dropped,
        });
        let before = self.included.len();
        self.included.retain(|id| keep.contains(id));
        self.included_at.retain(|id, _| keep.contains(id));
        self.symbol_of.retain(|id, _| keep.contains(id));
        self.manual_ids.retain(|id| keep.contains(id));
        before != self.included.len()
    }
}

fn beijing_day(at: DateTime<Utc>) -> NaiveDate {
    at.with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).expect("UTC+8"))
        .date_naive()
}

pub fn ended_position(position: &PairPosition) -> bool {
    matches!(
        position.status,
        PositionStatus::Closed | PositionStatus::Unwound
    )
}

pub fn settled_net(position: &PairPosition) -> Option<Decimal> {
    position.realized_source.is_some().then(|| {
        position.realized_pnl_usdt - position.realized_fee_usdt
            + position.realized_funding_usdt.unwrap_or_default()
    })
}

pub(crate) fn filled_volume(replayed: &Replayed, position_id: &str) -> Decimal {
    let prefix = format!("{position_id}-");
    replayed
        .orders
        .values()
        .filter(|order| {
            order.order.client_order_id.0.starts_with(&prefix)
                && (order.filled_usdt > Decimal::ZERO || order.status == OrderStatus::Filled)
        })
        .map(|order| order.filled_usdt)
        .sum()
}

/// 仓位写入台账后调用：若已结束且盈亏结算清楚，则计入累计。
pub async fn sync_after_position_write(
    ledger: &Ledger,
    position: &PairPosition,
    snapshot: Option<&crate::ledger::Replayed>,
) -> std::io::Result<bool> {
    if settled_net(position).is_none() || !ended_position(position) {
        return Ok(false);
    }
    let path = CumulativePnl::path_for(ledger.path());
    let mut store = CumulativePnl::load_or_bootstrap(ledger).await?;
    // 盈亏、手续费、资金费在仓位上。快照里有开仓单就用它，否则只按仓位号扫这一笔。
    let fills = crate::ledger::orders_for_position(ledger.path(), snapshot, &position.id);
    if !store.record_from_position(position, &fills, filled_volume(&fills, &position.id)) {
        return Ok(false);
    }
    store.save(&path).await?;
    Ok(true)
}

/// 资金费更正后调用。
pub async fn sync_funding_correction(
    ledger: &Ledger,
    position_id: &str,
    previous: Option<Decimal>,
    actual: Decimal,
    venue_deltas: &[(String, Decimal)],
) -> std::io::Result<bool> {
    let path = CumulativePnl::path_for(ledger.path());
    let mut store = CumulativePnl::load_or_bootstrap(ledger).await?;
    if !store.apply_funding_delta_venues(position_id, previous, actual, venue_deltas) {
        return Ok(false);
    }
    store.save(&path).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RealizedSource, Strategy};
    use arb_core::Symbol;
    use rust_decimal_macros::dec;

    fn closed(
        id: &str,
        base: &str,
        pnl: Decimal,
        fee: Decimal,
        funding: Option<Decimal>,
    ) -> PairPosition {
        PairPosition {
            id: id.into(),
            symbol: Symbol::perp(base, "USDT"),
            strategy: Strategy::Funding,
            long: None,
            short: None,
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status: PositionStatus::Closed,
            opened_at: Utc::now(),
            closed_at: Some(Utc::now()),
            note: None,
            margin_mode: crate::MarginMode::Isolated,
            leverage: None,
            rules: Default::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: pnl,
            realized_fee_usdt: fee,
            realized_source: Some(RealizedSource::Executor),
            realized_funding_usdt: funding,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            close_report: None,
            planned_long: None,
            planned_short: None,
            settled_legs: Vec::new(),
            funding_legs: Vec::new(),
            origin: None,
        }
    }

    #[test]
    fn settlements_accumulate_once_and_funding_corrections_apply_delta() {
        let mut store = CumulativePnl::default();
        let a = closed("a", "ETH", dec!(1.4), Decimal::ZERO, Some(dec!(0.1)));
        let b = closed("b", "BTC", dec!(-0.5), Decimal::ZERO, None);
        let replayed = Replayed::default();
        assert!(store.record_from_position(&a, &replayed, dec!(100)));
        assert!(!store.record_from_position(&a, &replayed, dec!(100)));
        assert!(store.record_from_position(&b, &replayed, dec!(50)));
        assert_eq!(store.net_usdt, dec!(1.0));
        assert_eq!(store.settled, 2);
        assert_eq!(store.volume_usdt, dec!(150));
        assert_eq!(store.symbols["ETH/USDT"].opens, 1);
        assert_eq!(store.symbols["BTC/USDT"].volume_usdt, dec!(50));
        assert!(store.apply_funding_delta("a", Some(dec!(0.1)), dec!(0.4)));
        assert_eq!(store.net_usdt, dec!(1.3));
        assert_eq!(store.funding_usdt, dec!(0.4));
        assert_eq!(store.symbols["ETH/USDT"].funding_usdt, dec!(0.4));
        assert!(!store.apply_funding_delta("missing", None, dec!(1)));
    }

    #[test]
    fn manual_positions_are_marked_without_being_added_twice() {
        let mut manual = closed("m", "USO", dec!(2), dec!(0.5), Some(dec!(0.25)));
        manual.origin = Some("manual".into());
        let auto = closed("a", "USO", dec!(1), Decimal::ZERO, None);
        let mut store = CumulativePnl::default();
        let replayed = Replayed::default();
        assert!(store.record_from_position(&manual, &replayed, dec!(10)));
        assert!(store.record_from_position(&auto, &replayed, dec!(10)));
        assert!(!store.record_from_position(&manual, &replayed, dec!(10)));
        assert_eq!(store.net_usdt, dec!(2.75));
        assert_eq!(store.manual_net_usdt, dec!(1.75));
        assert_eq!(store.manual_settled, 1);
        assert!(store.apply_funding_delta("m", Some(dec!(0.25)), dec!(0.5)));
        assert_eq!(store.manual_net_usdt, dec!(2.0));
        assert_eq!(store.net_usdt, dec!(3.0));
    }

    #[test]
    fn bootstrap_sums_settled_positions_only() {
        let mut open = closed("open", "BTC", dec!(9), Decimal::ZERO, None);
        open.status = PositionStatus::Open;
        open.realized_source = None;
        let mut unknown = closed("u", "BTC", Decimal::ZERO, Decimal::ZERO, None);
        unknown.realized_source = None;
        let a = closed("a", "BTC", dec!(3), dec!(0.2), Some(dec!(0.1)));
        let store = CumulativePnl::bootstrap_from_positions([&a, &open, &unknown].into_iter());
        assert_eq!(store.net_usdt, dec!(2.9));
        assert_eq!(store.fee_usdt, dec!(0.2));
        assert_eq!(store.settled, 1);
        assert!(store.included.contains("a"));
        assert!(!store.included.contains("u"));
        assert_eq!(store.symbols["BTC/USDT"].opens, 1);
    }

    #[test]
    fn a_new_symbol_gets_its_own_row() {
        let mut store = CumulativePnl::default();
        assert!(
            store.record_from_position(
                &closed("a", "ETH", dec!(1), dec!(0.1), None),
                &Replayed::default(),
                dec!(10),
            )
        );
        assert!(
            store.record_from_position(
                &closed("b", "SOL", dec!(2), dec!(0.2), None),
                &Replayed::default(),
                dec!(20),
            )
        );
        assert_eq!(store.symbols.len(), 2);
        assert_eq!(store.symbols["SOL/USDT"].net_usdt, dec!(1.8));
        assert_eq!(
            store.net_usdt,
            store.symbols["ETH/USDT"].net_usdt + store.symbols["SOL/USDT"].net_usdt
        );
    }

    #[test]
    fn venue_parts_keep_each_fill_and_count_one_position_once() {
        use crate::types::{ClientOrderId, MarginMode, NewOrder, OrderState, SettledLeg};
        use arb_core::{Side, Venue};

        let mut position = closed("p", "USO", dec!(2), dec!(0.15), None);
        position.status = PositionStatus::Unwound;
        position.settled_legs = vec![
            SettledLeg {
                venue: Venue::Arcus,
                side: Side::Buy,
                quantity: Decimal::ONE,
                entry_price: dec!(10),
                exit_price: dec!(11),
                price_pnl_usdt: dec!(1),
                fee_usdt: dec!(0.1),
            },
            SettledLeg {
                venue: Venue::Arcus,
                side: Side::Buy,
                quantity: Decimal::ONE,
                entry_price: dec!(10),
                exit_price: dec!(10),
                price_pnl_usdt: Decimal::ZERO,
                fee_usdt: dec!(0.05),
            },
            SettledLeg {
                venue: Venue::LighterRh,
                side: Side::Sell,
                quantity: Decimal::ONE,
                entry_price: dec!(11),
                exit_price: dec!(10),
                price_pnl_usdt: dec!(1),
                fee_usdt: Decimal::ZERO,
            },
        ];
        let mut replayed = Replayed::default();
        let order = |id: &str, venue: Venue, filled: Decimal| {
            let mut state = OrderState::new(NewOrder {
                client_order_id: ClientOrderId(id.into()),
                venue,
                symbol: Symbol::perp("USO", "USDT"),
                side: Side::Buy,
                notional_usdt: filled,
                quantity: None,
                limit_price: None,
                reduce_only: false,
                margin_mode: MarginMode::Isolated,
                leverage: None,
            });
            state.status = OrderStatus::Filled;
            state.filled_usdt = filled;
            state.fee_usdt = if venue == Venue::Arcus {
                dec!(0.04)
            } else {
                Decimal::ZERO
            };
            state
        };
        replayed
            .orders
            .insert("a".into(), order("p-buy-0", Venue::Arcus, dec!(100)));
        replayed.orders.insert(
            "b".into(),
            order("p-sell-0", Venue::LighterRh, dec!(40)),
        );
        let mut parts = venue_parts(&replayed, &position);
        parts.sort_by(|left, right| left.venue.cmp(&right.venue));
        assert_eq!(parts[0].venue, "arcus");
        assert_eq!(parts[0].volume_usdt, dec!(100));
        assert_eq!(parts[0].opens, 1);
        assert_eq!(parts[0].unwound, 1);
        assert_eq!(parts[0].unwound_usdt, dec!(100));
        assert_eq!(parts[0].fee_usdt, dec!(0.15));
        assert_eq!(parts[0].funding_usdt, Decimal::ZERO);
        assert_eq!(parts[1].unwound_usdt, dec!(40));
        assert_eq!(parts[1].opens, 1);
        position.realized_funding_usdt = Some(dec!(0.3));
        position.set_booked_funding(vec![
            crate::types::VenueFunding {
                venue: Venue::Arcus,
                usdt: dec!(0.2),
            },
            crate::types::VenueFunding {
                venue: Venue::LighterRh,
                usdt: dec!(0.1),
            },
        ]);
        let booked = venue_parts(&replayed, &position);
        let arcus = booked.iter().find(|part| part.venue == "arcus").unwrap();
        let lighter = booked.iter().find(|part| part.venue == "lighter-rh").unwrap();
        assert_eq!(arcus.funding_usdt, dec!(0.2));
        assert_eq!(arcus.net_usdt, dec!(1) - dec!(0.15) + dec!(0.2));
        assert_eq!(lighter.funding_usdt, dec!(0.1));
        assert_eq!(lighter.opens, 1);
        let mut single = closed("one", "USO", dec!(1), dec!(0.04), Some(dec!(0.3)));
        single.status = PositionStatus::Closed;
        let mut only = Replayed::default();
        only.orders
            .insert("a".into(), order("one-buy-0", Venue::Arcus, dec!(10)));
        let alone = venue_parts(&only, &single);
        assert_eq!(alone.len(), 1);
        assert_eq!(alone[0].fee_usdt, dec!(0.04));
        assert_eq!(alone[0].funding_usdt, dec!(0.3));
        assert_eq!(alone[0].net_usdt, dec!(0.3) - dec!(0.04));
    }

    /// 记过的 id 多过 1024 笔就收成水位。合计不变，更早的同一笔不会再加一次。
    #[test]
    fn old_ids_collapse_into_a_watermark_without_double_counting() {
        let mut store = CumulativePnl::default();
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        for index in 0..1100 {
            let mut position = closed(
                &format!("p{index:04}"),
                "BTC",
                dec!(0.01),
                Decimal::ZERO,
                None,
            );
            position.closed_at = Some(start + chrono::Duration::seconds(index));
            position.opened_at = position.closed_at.unwrap();
            assert!(store.record_from_position(&position, &Replayed::default(), Decimal::ONE));
        }
        let net = store.net_usdt;
        let settled = store.settled;
        assert!(store.prune_tracked_ids(Utc::now()));
        assert!(store.included.len() <= 1025, "{}", store.included.len());
        assert!(store.included_through.is_some());
        assert!(!store.included.contains("p0000"));
        assert_eq!(store.net_usdt, net);
        assert_eq!(store.settled, settled);
        let mut oldest = closed("p0000", "BTC", dec!(0.01), Decimal::ZERO, None);
        oldest.closed_at = Some(start);
        oldest.opened_at = start;
        assert!(!store.record_from_position(&oldest, &Replayed::default(), Decimal::ONE));
        assert_eq!(store.net_usdt, net);
        assert_eq!(store.settled, settled);
    }
}
