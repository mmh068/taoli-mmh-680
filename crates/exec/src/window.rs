//! 近 7 日、今日、今日按币种的滚动存档。
//!
//! 文件是 `{台账路径}.window-pnl.json`。按北京日历日逐笔记账，只留最近 7 天（含今天）。
//! 更早的日期在读取和写入时剔除，不把整本台账再加一遍。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::cumulative::{ended_position, filled_volume, settled_net};
use crate::desk::{LedgerBucket, SymbolLedgerStats, beijing_date};
use crate::ledger::Replayed;
use crate::types::{PairPosition, PositionStatus};

const VERSION: u32 = 1;
const KEEP_DAYS: i64 = 7;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DaySymbol {
    volume_usdt: Decimal,
    opens: u64,
    net_usdt: Decimal,
    fee_usdt: Decimal,
    funding_usdt: Decimal,
    unknown: u64,
    unwound: u64,
    unwound_usdt: Decimal,
}

impl Default for DaySymbol {
    fn default() -> Self {
        Self {
            volume_usdt: Decimal::ZERO,
            opens: 0,
            net_usdt: Decimal::ZERO,
            fee_usdt: Decimal::ZERO,
            funding_usdt: Decimal::ZERO,
            unknown: 0,
            unwound: 0,
            unwound_usdt: Decimal::ZERO,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DayRollup {
    volume_usdt: Decimal,
    opens: u64,
    net_usdt: Decimal,
    fee_usdt: Decimal,
    funding_usdt: Decimal,
    unknown: u64,
    unwound: u64,
    unwound_usdt: Decimal,
    symbols: BTreeMap<String, DaySymbol>,
    /// 当天计入的仓位 id。日期过期时一并剔除，避免 included 只增不减。
    #[serde(default)]
    ids: BTreeSet<String>,
}

impl Default for DayRollup {
    fn default() -> Self {
        Self {
            volume_usdt: Decimal::ZERO,
            opens: 0,
            net_usdt: Decimal::ZERO,
            fee_usdt: Decimal::ZERO,
            funding_usdt: Decimal::ZERO,
            unknown: 0,
            unwound: 0,
            unwound_usdt: Decimal::ZERO,
            symbols: BTreeMap::new(),
            ids: BTreeSet::new(),
        }
    }
}

/// 近 7 个北京日历日的滚动窗口。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowPnl {
    version: u32,
    /// 已经计入的仓位 id，避免同一笔重复加。
    included: BTreeSet<String>,
    /// 北京日历日 → 当天合计。只保留最近 7 天。
    days: BTreeMap<NaiveDate, DayRollup>,
    updated_at: DateTime<Utc>,
}

impl Default for WindowPnl {
    fn default() -> Self {
        Self {
            version: VERSION,
            included: BTreeSet::new(),
            days: BTreeMap::new(),
            updated_at: Utc::now(),
        }
    }
}

/// 台账页直接展示的三段。
#[derive(Debug, Clone)]
pub struct WindowView {
    pub week: LedgerBucket,
    pub today: LedgerBucket,
    pub today_symbols: Vec<SymbolLedgerStats>,
}

impl WindowPnl {
    pub fn path_for(ledger: &Path) -> PathBuf {
        let mut name = ledger.as_os_str().to_owned();
        name.push(".window-pnl.json");
        PathBuf::from(name)
    }

    /// 读存档；没有或读坏了就按当前回放引导。新结算的仓位补进去，过期日期剔除，有变化才写回。
    pub fn load_or_bootstrap(ledger_path: &Path, replayed: &Replayed, today: NaiveDate) -> Self {
        let path = Self::path_for(ledger_path);
        let mut store = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Self>(&bytes).ok())
            .unwrap_or_default();
        let mut changed = store.prune(today);
        for position in ended_in_window(replayed, today) {
            let volume = filled_volume(replayed, &position.id);
            changed |= store.record(position, volume, today);
        }
        if changed || !path.exists() {
            let _ = store.save_sync(&path);
        }
        store
    }

    pub fn view(&self, today: NaiveDate) -> WindowView {
        let from = today
            .checked_sub_signed(chrono::Duration::days(KEEP_DAYS - 1))
            .unwrap_or(today);
        let mut week = LedgerBucket::default();
        for (day, rollup) in &self.days {
            if *day >= from && *day <= today {
                add_day(&mut week, rollup);
            }
        }
        let today_rollup = self.days.get(&today);
        let today_bucket = today_rollup
            .map(|rollup| {
                let mut bucket = LedgerBucket::default();
                add_day(&mut bucket, rollup);
                bucket
            })
            .unwrap_or_default();
        let mut today_symbols: Vec<SymbolLedgerStats> = today_rollup
            .map(|rollup| {
                rollup
                    .symbols
                    .iter()
                    .map(|(symbol, row)| SymbolLedgerStats {
                        symbol: symbol.clone(),
                        stats: bucket_of(row),
                    })
                    .collect()
            })
            .unwrap_or_default();
        today_symbols.sort_by(|left, right| {
            right
                .stats
                .volume_usdt
                .cmp(&left.stats.volume_usdt)
                .then_with(|| left.symbol.cmp(&right.symbol))
        });
        WindowView {
            week: round_bucket(week),
            today: round_bucket(today_bucket),
            today_symbols,
        }
    }

    fn record(&mut self, position: &PairPosition, volume: Decimal, today: NaiveDate) -> bool {
        let Some(closed_at) = position.closed_at else {
            return false;
        };
        let day = beijing_date(closed_at);
        let from = today
            .checked_sub_signed(chrono::Duration::days(KEEP_DAYS - 1))
            .unwrap_or(today);
        if day < from || day > today || !self.included.insert(position.id.clone()) {
            return false;
        }
        let row = self.days.entry(day).or_default();
        row.ids.insert(position.id.clone());
        let symbol_name = position.symbol.to_string();
        if position.status == PositionStatus::Unwound {
            row.unwound += 1;
            row.unwound_usdt = (row.unwound_usdt + volume).round_dp(8);
            let symbol = row.symbols.entry(symbol_name.clone()).or_default();
            symbol.unwound += 1;
            symbol.unwound_usdt = (symbol.unwound_usdt + volume).round_dp(8);
        }
        match settled_net(position) {
            None => {
                row.unknown += 1;
                row.symbols.entry(symbol_name).or_default().unknown += 1;
            }
            Some(net) => {
                let fee = position.realized_fee_usdt;
                let funding = position.realized_funding_usdt.unwrap_or_default();
                add_settled(row, &symbol_name, volume, net, fee, funding);
            }
        }
        self.updated_at = Utc::now();
        true
    }

    fn prune(&mut self, today: NaiveDate) -> bool {
        let from = today
            .checked_sub_signed(chrono::Duration::days(KEEP_DAYS - 1))
            .unwrap_or(today);
        let before = self.days.len();
        let mut dropped = BTreeSet::new();
        self.days.retain(|day, row| {
            let keep = *day >= from && *day <= today;
            if !keep {
                dropped.append(&mut row.ids);
            }
            keep
        });
        self.included.retain(|id| !dropped.contains(id));
        before != self.days.len()
    }

    fn save_sync(&self, path: &Path) -> std::io::Result<()> {
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
}

fn ended_in_window(replayed: &Replayed, today: NaiveDate) -> Vec<&PairPosition> {
    let from = today
        .checked_sub_signed(chrono::Duration::days(KEEP_DAYS - 1))
        .unwrap_or(today);
    let mut ended: Vec<&PairPosition> = replayed
        .positions
        .values()
        .filter(|position| {
            ended_position(position)
                && position.closed_at.is_some_and(|at| {
                    let day = beijing_date(at);
                    day >= from && day <= today
                })
        })
        .collect();
    ended.sort_by(|left, right| left.id.cmp(&right.id));
    ended
}

fn add_settled(
    day: &mut DayRollup,
    symbol_name: &str,
    volume: Decimal,
    net: Decimal,
    fee: Decimal,
    funding: Decimal,
) {
    day.opens += 1;
    day.volume_usdt = (day.volume_usdt + volume).round_dp(8);
    day.net_usdt = (day.net_usdt + net).round_dp(8);
    day.fee_usdt = (day.fee_usdt + fee).round_dp(8);
    day.funding_usdt = (day.funding_usdt + funding).round_dp(8);
    let symbol = day.symbols.entry(symbol_name.to_string()).or_default();
    symbol.opens += 1;
    symbol.volume_usdt = (symbol.volume_usdt + volume).round_dp(8);
    symbol.net_usdt = (symbol.net_usdt + net).round_dp(8);
    symbol.fee_usdt = (symbol.fee_usdt + fee).round_dp(8);
    symbol.funding_usdt = (symbol.funding_usdt + funding).round_dp(8);
}

fn add_day(bucket: &mut LedgerBucket, day: &DayRollup) {
    bucket.volume_usdt += day.volume_usdt;
    bucket.opens += day.opens as usize;
    bucket.net_usdt += day.net_usdt;
    bucket.fee_usdt += day.fee_usdt;
    bucket.funding_usdt += day.funding_usdt;
    bucket.settled += day.opens as usize;
    bucket.unknown += day.unknown as usize;
    bucket.unwound += day.unwound as usize;
    bucket.unwound_usdt += day.unwound_usdt;
}

fn bucket_of(row: &DaySymbol) -> LedgerBucket {
    round_bucket(LedgerBucket {
        volume_usdt: row.volume_usdt,
        opens: row.opens as usize,
        net_usdt: row.net_usdt,
        fee_usdt: row.fee_usdt,
        funding_usdt: row.funding_usdt,
        settled: row.opens as usize,
        unknown: row.unknown as usize,
        unwound: row.unwound as usize,
        unwound_usdt: row.unwound_usdt,
    })
}

fn round_bucket(mut bucket: LedgerBucket) -> LedgerBucket {
    bucket.volume_usdt = bucket.volume_usdt.round_dp(8);
    bucket.net_usdt = bucket.net_usdt.round_dp(8);
    bucket.fee_usdt = bucket.fee_usdt.round_dp(8);
    bucket.funding_usdt = bucket.funding_usdt.round_dp(8);
    bucket.unwound_usdt = bucket.unwound_usdt.round_dp(8);
    bucket
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RealizedSource, Strategy};
    use arb_core::Symbol;
    use rust_decimal_macros::dec;

    fn ended(id: &str, base: &str, day: NaiveDate, unwound: bool) -> PairPosition {
        PairPosition {
            margin_mode: crate::MarginMode::Isolated,
            id: id.into(),
            symbol: Symbol::perp(base, "USDT"),
            strategy: Strategy::Spread,
            long: None,
            short: None,
            entry_basis_pct: dec!(0.1),
            expected_round_trip_cost: dec!(0.002),
            status: if unwound {
                PositionStatus::Unwound
            } else {
                PositionStatus::Closed
            },
            opened_at: Utc::now(),
            closed_at: Some(day.and_hms_opt(4, 0, 0).unwrap().and_utc()),
            note: None,
            leverage: None,
            rules: crate::TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: dec!(1),
            realized_fee_usdt: dec!(0.1),
            realized_source: Some(RealizedSource::Executor),
            realized_funding_usdt: Some(dec!(0.2)),
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            origin: None,
        }
    }

    #[test]
    fn old_days_drop_out_and_today_is_split_by_symbol() {
        let today = NaiveDate::from_ymd_opt(2026, 7, 23).unwrap();
        let mut replayed = Replayed::default();
        let fresh = ended("new", "USO", today, true);
        let stale = ended(
            "old",
            "USO",
            today.checked_sub_signed(chrono::Duration::days(8)).unwrap(),
            false,
        );
        replayed.positions.insert(fresh.id.clone(), fresh);
        replayed.positions.insert(stale.id.clone(), stale);
        let path = std::env::temp_dir().join(format!(
            "window-test-{}-{}.jsonl",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let store = WindowPnl::load_or_bootstrap(&path, &replayed, today);
        let view = store.view(today);
        assert_eq!(view.today.unwound, 1);
        assert_eq!(view.today.opens, 1);
        assert_eq!(view.week.opens, 1);
        assert_eq!(view.today_symbols.len(), 1);
        assert_eq!(view.today_symbols[0].symbol, "USO/USDT");
        assert!(
            !store
                .days
                .contains_key(&today.checked_sub_signed(chrono::Duration::days(8)).unwrap())
        );
        assert!(!store.included.contains("old"));
        let again = WindowPnl::load_or_bootstrap(&path, &replayed, today);
        assert_eq!(again.included.len(), 1, "同一笔不重复计入");
        let _ = std::fs::remove_file(WindowPnl::path_for(&path));
        let _ = std::fs::remove_file(&path);
    }
}
