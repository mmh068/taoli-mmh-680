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
    /// 当天按成交交易所。旧存档没有，读到时按当前台账补一次。
    #[serde(default)]
    venues: BTreeMap<String, DaySymbol>,
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
            venues: BTreeMap::new(),
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
    pub week_venues: Vec<SymbolLedgerStats>,
    pub today_venues: Vec<SymbolLedgerStats>,
}

/// 平仓入账后调用。用这次已经加载的回放记近 7 日，不读整本。
///
/// 快照里没有这笔开仓单时，只按仓位号扫它的订单。
pub fn note_replay(ledger_path: &Path, replayed: &Replayed) {
    let today = beijing_date(Utc::now());
    let _ = WindowPnl::record_from_snapshot(ledger_path, replayed, today);
}

/// 资金费更正后调用。仓位不在这 7 日里就不动；下次入账仍读台账上更正后的数字。
pub fn note_funding_delta(
    ledger_path: &Path,
    position_id: &str,
    symbol: &str,
    previous: Option<Decimal>,
    actual: Decimal,
    venue_deltas: &[(String, Decimal)],
) {
    let path = WindowPnl::path_for(ledger_path);
    let Ok(bytes) = std::fs::read(&path) else {
        return;
    };
    let Ok(mut store) = serde_json::from_slice::<WindowPnl>(&bytes) else {
        return;
    };
    if store.apply_funding_delta(position_id, symbol, previous, actual, venue_deltas) {
        let _ = store.save_sync(&path);
    }
}

impl WindowPnl {
    pub fn path_for(ledger: &Path) -> PathBuf {
        let mut name = ledger.as_os_str().to_owned();
        name.push(".window-pnl.json");
        PathBuf::from(name)
    }

    /// 入账时调用。只记这次回放里、窗口内还没记的仓位，不读整本，也不重写旧交易所行。
    fn record_from_snapshot(ledger_path: &Path, replayed: &Replayed, today: NaiveDate) -> Self {
        let path = Self::path_for(ledger_path);
        let mut store = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Self>(&bytes).ok())
            .unwrap_or_default();
        let mut changed = store.prune(today);
        for position in ended_in_window(replayed, today) {
            let fills =
                crate::ledger::orders_for_position(ledger_path, Some(replayed), &position.id);
            let volume = filled_volume(&fills, &position.id);
            changed |= store.record(position, &fills, volume, today);
        }
        if changed || !path.exists() {
            let _ = store.save_sync(&path);
        }
        store
    }

    /// 窗口文件还不存在时，按传入的回放建一次。平仓入账不走这里。
    pub fn record_from_full(ledger_path: &Path, replayed: &Replayed, today: NaiveDate) -> Self {
        let path = Self::path_for(ledger_path);
        let mut store = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Self>(&bytes).ok())
            .unwrap_or_default();
        let mut changed = store.prune(today);
        for position in ended_in_window(replayed, today) {
            let volume = filled_volume(replayed, &position.id);
            changed |= store.record(position, replayed, volume, today);
        }
        if changed || !path.exists() {
            let _ = store.save_sync(&path);
        }
        store
    }

    /// 台账页用。有存档就只读。没有文件才按整本台账建一次。
    pub fn load_for_page(ledger_path: &Path, today: NaiveDate) -> Self {
        let path = Self::path_for(ledger_path);
        if path.exists() {
            return Self::load_for_display(ledger_path, today);
        }
        let Ok(replayed) = crate::ledger::replay_full_sync(ledger_path) else {
            return Self::load_for_display(ledger_path, today);
        };
        Self::record_from_full(ledger_path, &replayed, today)
    }

    /// 只读存档。没有或读坏了返回空窗口，不用当前回放重算。
    pub fn load_for_display(ledger_path: &Path, today: NaiveDate) -> Self {
        let path = Self::path_for(ledger_path);
        std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Self>(&bytes).ok())
            .map(|mut store| {
                store.prune(today);
                store
            })
            .unwrap_or_default()
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
            week_venues: venues_between(self, from, today),
            today_venues: venues_between(self, today, today),
        }
    }

    fn record(
        &mut self,
        position: &PairPosition,
        replayed: &Replayed,
        volume: Decimal,
        today: NaiveDate,
    ) -> bool {
        let Some(closed_at) = position.closed_at else {
            return false;
        };
        let day = beijing_date(closed_at);
        let from = today
            .checked_sub_signed(chrono::Duration::days(KEEP_DAYS - 1))
            .unwrap_or(today);
        if day < from || day > today {
            return false;
        }
        // 盈亏核算清楚才进统计。外部平仓会先写一条还没核算的 Closed，这时不能占 id。
        let Some(net) = settled_net(position) else {
            return false;
        };
        if !self.included.insert(position.id.clone()) {
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
        let fee = position.realized_fee_usdt;
        let funding = position.realized_funding_usdt.unwrap_or_default();
        add_settled(row, &symbol_name, volume, net, fee, funding);
        add_venues(row, replayed, position);
        self.updated_at = Utc::now();
        true
    }

    fn apply_funding_delta(
        &mut self,
        position_id: &str,
        symbol: &str,
        previous: Option<Decimal>,
        actual: Decimal,
        venue_deltas: &[(String, Decimal)],
    ) -> bool {
        let Some(row) = self
            .days
            .values_mut()
            .find(|row| row.ids.contains(position_id))
        else {
            return false;
        };
        let delta = actual - previous.unwrap_or(Decimal::ZERO);
        row.net_usdt = (row.net_usdt + delta).round_dp(8);
        row.funding_usdt = (row.funding_usdt + delta).round_dp(8);
        let symbol_row = row.symbols.entry(symbol.to_string()).or_default();
        symbol_row.net_usdt = (symbol_row.net_usdt + delta).round_dp(8);
        symbol_row.funding_usdt = (symbol_row.funding_usdt + delta).round_dp(8);
        for (venue, venue_delta) in venue_deltas {
            if *venue_delta == Decimal::ZERO {
                continue;
            }
            let venue_row = row.venues.entry(venue.clone()).or_default();
            venue_row.net_usdt = (venue_row.net_usdt + venue_delta).round_dp(8);
            venue_row.funding_usdt = (venue_row.funding_usdt + venue_delta).round_dp(8);
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

/// 一笔仓位拆进当天的交易所行。口径与累计存档相同。
fn add_venues(day: &mut DayRollup, replayed: &Replayed, position: &PairPosition) {
    for part in crate::cumulative::venue_parts(replayed, position) {
        let row = day.venues.entry(part.venue).or_default();
        row.volume_usdt = (row.volume_usdt + part.volume_usdt).round_dp(8);
        row.opens += part.opens;
        row.net_usdt = (row.net_usdt + part.net_usdt).round_dp(8);
        row.fee_usdt = (row.fee_usdt + part.fee_usdt).round_dp(8);
        row.funding_usdt = (row.funding_usdt + part.funding_usdt).round_dp(8);
        row.unwound += part.unwound;
        row.unwound_usdt = (row.unwound_usdt + part.unwound_usdt).round_dp(8);
    }
}

fn venues_between(store: &WindowPnl, from: NaiveDate, to: NaiveDate) -> Vec<SymbolLedgerStats> {
    let mut rows: BTreeMap<String, LedgerBucket> = BTreeMap::new();
    for (day, rollup) in &store.days {
        if *day < from || *day > to {
            continue;
        }
        for (venue, row) in &rollup.venues {
            let bucket = rows.entry(venue.clone()).or_default();
            bucket.volume_usdt += row.volume_usdt;
            bucket.opens += row.opens as usize;
            bucket.net_usdt += row.net_usdt;
            bucket.fee_usdt += row.fee_usdt;
            bucket.funding_usdt += row.funding_usdt;
            bucket.settled += row.opens as usize;
            bucket.unknown += row.unknown as usize;
            bucket.unwound += row.unwound as usize;
            bucket.unwound_usdt += row.unwound_usdt;
        }
    }
    let mut venues: Vec<SymbolLedgerStats> = rows
        .into_iter()
        .map(|(symbol, stats)| SymbolLedgerStats {
            symbol,
            stats: round_bucket(stats),
        })
        .collect();
    venues.sort_by(|left, right| {
        right
            .stats
            .volume_usdt
            .cmp(&left.stats.volume_usdt)
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    venues
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
        price_pnl_usdt: Decimal::ZERO,
    })
}

fn round_bucket(mut bucket: LedgerBucket) -> LedgerBucket {
    bucket.volume_usdt = bucket.volume_usdt.round_dp(8);
    bucket.net_usdt = bucket.net_usdt.round_dp(8);
    bucket.fee_usdt = bucket.fee_usdt.round_dp(8);
    bucket.funding_usdt = bucket.funding_usdt.round_dp(8);
    bucket.unwound_usdt = bucket.unwound_usdt.round_dp(8);
    bucket.fill_price_pnl();
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
            planned_long: None,
            planned_short: None,
            settled_legs: Vec::new(),
            funding_legs: Vec::new(),
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
        let store = WindowPnl::record_from_full(&path, &replayed, today);
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
        let again = WindowPnl::record_from_full(&path, &replayed, today);
        assert_eq!(again.included.len(), 1, "同一笔不重复计入");
        let _ = std::fs::remove_file(WindowPnl::path_for(&path));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn display_reads_the_file_and_does_not_rebuild_from_a_thin_replay() {
        let today = NaiveDate::from_ymd_opt(2026, 7, 23).unwrap();
        let position = ended("new", "USO", today, false);
        let mut replayed = Replayed::default();
        replayed.positions.insert(position.id.clone(), position);
        let path = std::env::temp_dir().join(format!(
            "window-display-{}-{}.jsonl",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        WindowPnl::record_from_full(&path, &replayed, today);
        let shown = WindowPnl::load_for_display(&path, today);
        assert_eq!(shown.view(today).today.opens, 1);
        let empty = WindowPnl::load_for_display(&path, today);
        assert_eq!(empty.view(today).today.opens, 1, "打开页面不再用回放重算");
        let _ = std::fs::remove_file(WindowPnl::path_for(&path));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn venues_follow_the_window_file_and_are_not_added_twice() {
        use crate::types::{ClientOrderId, MarginMode, NewOrder, OrderState, OrderStatus, SettledLeg};
        use arb_core::{Side, Venue};

        let today = NaiveDate::from_ymd_opt(2026, 7, 23).unwrap();
        let mut position = ended("new", "USO", today, false);
        position.settled_legs = vec![
            SettledLeg {
                venue: Venue::Arcus,
                side: Side::Buy,
                quantity: dec!(1),
                entry_price: dec!(10),
                exit_price: dec!(11),
                price_pnl_usdt: dec!(1),
                fee_usdt: dec!(0.1),
            },
            SettledLeg {
                venue: Venue::LighterRh,
                side: Side::Sell,
                quantity: dec!(1),
                entry_price: dec!(11),
                exit_price: dec!(10),
                price_pnl_usdt: dec!(1),
                fee_usdt: dec!(0.05),
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
            state
        };
        replayed
            .orders
            .insert("a".into(), order("new-buy-0", Venue::Arcus, dec!(100)));
        replayed.orders.insert(
            "b".into(),
            order("new-sell-0", Venue::LighterRh, dec!(40)),
        );
        replayed.positions.insert(position.id.clone(), position);
        let path = std::env::temp_dir().join(format!(
            "window-venue-{}-{}.jsonl",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let store = WindowPnl::record_from_full(&path, &replayed, today);
        let view = store.view(today);
        assert_eq!(view.today_venues.len(), 2);
        assert_eq!(view.today_venues[0].symbol, "arcus");
        assert_eq!(view.today_venues[0].stats.volume_usdt, dec!(100));
        assert_eq!(view.today_venues[0].stats.net_usdt, dec!(0.9));
        assert_eq!(view.week_venues[1].symbol, "lighter-rh");
        assert_eq!(view.week_venues[1].stats.volume_usdt, dec!(40));
        let again = WindowPnl::record_from_full(&path, &replayed, today);
        assert_eq!(again.view(today).today_venues[0].stats.volume_usdt, dec!(100));
        let _ = std::fs::remove_file(WindowPnl::path_for(&path));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unsettled_close_is_not_counted_until_pnl_is_booked() {
        let today = NaiveDate::from_ymd_opt(2026, 11, 9).unwrap();
        let mut position = ended("live-ext", "AAPL", today, false);
        position.realized_source = None;
        let mut replayed = Replayed::default();
        replayed
            .positions
            .insert(position.id.clone(), position.clone());
        let path = std::env::temp_dir().join(format!(
            "window-unsettled-{}-{}.jsonl",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let early = WindowPnl::record_from_full(&path, &replayed, today);
        assert!(!early.included.contains("live-ext"));
        assert_eq!(early.view(today).today.opens, 0);
        assert!(early.view(today).today_venues.is_empty());

        position.realized_source = Some(RealizedSource::VenueFills);
        replayed.positions.insert(position.id.clone(), position);
        let booked = WindowPnl::record_from_full(&path, &replayed, today);
        assert_eq!(booked.view(today).today.opens, 1);
        assert_eq!(booked.view(today).today.net_usdt, dec!(1.1));
        let again = WindowPnl::record_from_full(&path, &replayed, today);
        assert_eq!(again.view(today).today.opens, 1);
        let _ = std::fs::remove_file(WindowPnl::path_for(&path));
        let _ = std::fs::remove_file(&path);
    }
}
