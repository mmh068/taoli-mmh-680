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

use chrono::{DateTime, Utc};
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
    /// 仓位 id → 币种，资金费更正时找到对应行。
    #[serde(default)]
    pub symbol_of: BTreeMap<String, String>,
    /// 按币种滚动合计。
    #[serde(default)]
    pub symbols: BTreeMap<String, SymbolRollup>,
    /// 手动信号已结算净额。已包含在 `net_usdt` 里，只是单独标注。
    #[serde(default)]
    pub manual_net_usdt: Decimal,
    /// 上面这项包含的笔数。
    #[serde(default)]
    pub manual_settled: u64,
    /// 已经计入手动合计的仓位 id。旧存档升级时用来避免重复。
    #[serde(default)]
    pub manual_ids: BTreeSet<String>,
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
            symbol_of: BTreeMap::new(),
            symbols: BTreeMap::new(),
            manual_net_usdt: Decimal::ZERO,
            manual_settled: 0,
            manual_ids: BTreeSet::new(),
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

    /// 读已有存档；没有就按台账引导一次并落盘。
    pub async fn load_or_bootstrap(ledger: &Ledger) -> std::io::Result<Self> {
        let (replayed, _) = ledger.replay().await?;
        Ok(Self::load_or_bootstrap_from_replay(
            ledger.path(),
            &replayed,
        ))
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

    /// 有文件就读；没有就按当前回放引导并尽量落盘。持仓页 / 台账页等同步路径用。
    pub fn load_or_bootstrap_from_replay(ledger_path: &Path, replayed: &Replayed) -> Self {
        let path = Self::path_for(ledger_path);
        if path.exists() {
            if let Ok(mut value) = Self::load_sync(&path) {
                let mut changed = value.upgrade_symbol_rollup(replayed);
                changed |= value.upgrade_manual(replayed);
                if changed {
                    let _ = value.save_sync(&path);
                }
                return value;
            }
        }
        let value = Self::bootstrap_from_replay(replayed);
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
                value.record_from_position(position, filled_volume(replayed, &position.id));
            }
        }
        value.updated_at = Utc::now();
        value
    }

    pub fn bootstrap_from_positions<'a>(positions: impl Iterator<Item = &'a PairPosition>) -> Self {
        let mut value = Self::default();
        for position in positions {
            if settled_net(position).is_some() {
                value.record_from_position(position, Decimal::ZERO);
            }
        }
        value.updated_at = Utc::now();
        value
    }

    /// 旧存档只有总净额、没有按币种行：用当前台账把能对上的币种行补上，不改已经计入的净额。
    fn upgrade_symbol_rollup(&mut self, replayed: &Replayed) -> bool {
        if self.version >= VERSION && !self.symbols.is_empty() {
            return false;
        }
        if self.included.is_empty() {
            self.version = VERSION;
            return true;
        }
        for position in replayed.positions.values() {
            if !self.included.contains(&position.id) {
                continue;
            }
            let Some(net) = settled_net(position) else {
                continue;
            };
            let fee = position.realized_fee_usdt;
            let funding = position.realized_funding_usdt.unwrap_or_default();
            let volume = filled_volume(replayed, &position.id);
            let symbol = position.symbol.to_string();
            self.symbol_of.insert(position.id.clone(), symbol.clone());
            let row = self.symbols.entry(symbol).or_default();
            row.opens += 1;
            row.net_usdt = (row.net_usdt + net).round_dp(8);
            row.fee_usdt = (row.fee_usdt + fee).round_dp(8);
            row.funding_usdt = (row.funding_usdt + funding).round_dp(8);
            row.volume_usdt = (row.volume_usdt + volume).round_dp(8);
            self.fee_usdt = (self.fee_usdt + fee).round_dp(8);
            self.funding_usdt = (self.funding_usdt + funding).round_dp(8);
            self.volume_usdt = (self.volume_usdt + volume).round_dp(8);
        }
        self.version = VERSION;
        self.updated_at = Utc::now();
        true
    }

    /// 旧存档没有手动标记：把已经计入、且来源是手动的仓位补进单独合计，不改总净额。
    fn upgrade_manual(&mut self, replayed: &Replayed) -> bool {
        if self.version >= VERSION {
            return false;
        }
        for position in replayed.positions.values() {
            if position.origin.as_deref() == Some("manual") && self.included.contains(&position.id)
            {
                self.mark_manual(&position.id, settled_net(position).unwrap_or_default());
            }
        }
        self.version = VERSION;
        self.updated_at = Utc::now();
        true
    }

    fn mark_manual(&mut self, position_id: &str, net: Decimal) {
        if !self.manual_ids.insert(position_id.to_string()) {
            return;
        }
        self.manual_settled += 1;
        self.manual_net_usdt = (self.manual_net_usdt + net).round_dp(8);
    }

    /// 首次计入一笔已结算仓位。已计入过则返回 false。
    pub fn record_from_position(&mut self, position: &PairPosition, volume_usdt: Decimal) -> bool {
        let Some(net) = settled_net(position) else {
            return false;
        };
        let recorded = self.record_parts(
            &position.id,
            &position.symbol.to_string(),
            net,
            position.realized_fee_usdt,
            position.realized_funding_usdt.unwrap_or_default(),
            volume_usdt,
        );
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
) -> std::io::Result<bool> {
    if settled_net(position).is_none() || !ended_position(position) {
        return Ok(false);
    }
    let path = CumulativePnl::path_for(ledger.path());
    let mut store = CumulativePnl::load_or_bootstrap(ledger).await?;
    let (replayed, _) = ledger.replay().await?;
    if !store.record_from_position(position, filled_volume(&replayed, &position.id)) {
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
) -> std::io::Result<bool> {
    let path = CumulativePnl::path_for(ledger.path());
    let mut store = CumulativePnl::load_or_bootstrap(ledger).await?;
    if !store.apply_funding_delta(position_id, previous, actual) {
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
            origin: None,
        }
    }

    #[test]
    fn settlements_accumulate_once_and_funding_corrections_apply_delta() {
        let mut store = CumulativePnl::default();
        let a = closed("a", "ETH", dec!(1.4), Decimal::ZERO, Some(dec!(0.1)));
        let b = closed("b", "BTC", dec!(-0.5), Decimal::ZERO, None);
        assert!(store.record_from_position(&a, dec!(100)));
        assert!(!store.record_from_position(&a, dec!(100)));
        assert!(store.record_from_position(&b, dec!(50)));
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
        assert!(store.record_from_position(&manual, dec!(10)));
        assert!(store.record_from_position(&auto, dec!(10)));
        assert!(!store.record_from_position(&manual, dec!(10)));
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
            store.record_from_position(&closed("a", "ETH", dec!(1), dec!(0.1), None), dec!(10))
        );
        assert!(
            store.record_from_position(&closed("b", "SOL", dec!(2), dec!(0.2), None), dec!(20))
        );
        assert_eq!(store.symbols.len(), 2);
        assert_eq!(store.symbols["SOL/USDT"].net_usdt, dec!(1.8));
        assert_eq!(
            store.net_usdt,
            store.symbols["ETH/USDT"].net_usdt + store.symbols["SOL/USDT"].net_usdt
        );
    }
}
