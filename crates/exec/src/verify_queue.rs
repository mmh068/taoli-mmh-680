//! 待手动核实的仓位名单。
//!
//! 文件是 `{台账路径}.needs-verify.json`，跟在实盘台账旁边。只收录两类：
//! - 对账发现受牵连的敞口仓位（持仓页上对账不干净的那些）；
//! - 已结束但没有盈亏来源的仓位（未计入）。
//!
//! 第一次见到这份台账时，用当时已经读入内存的回放把未结算仓位收进来，并记下
//! `bootstrapped`。之后不再把整本台账翻一遍找未计入：新的未结算仓位在写入台账时
//! 增量更新，对账不干净的仓位只在每次对账时按仍有敞口的仓位增删。
//! 手动核实只读这份名单。对应台账补齐后，从名单里删除。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::cumulative::ended_position;
use crate::ledger::Replayed;
use crate::types::PairPosition;

const VERSION: u32 = 1;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct Entry {
    #[serde(default)]
    unsettled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reconciliation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Queue {
    version: u32,
    #[serde(default)]
    bootstrapped: bool,
    #[serde(default)]
    positions: BTreeMap<String, Entry>,
}

impl Default for Queue {
    fn default() -> Self {
        Self {
            version: VERSION,
            bootstrapped: false,
            positions: BTreeMap::new(),
        }
    }
}

fn path_for(ledger: &Path) -> PathBuf {
    let mut name = ledger.as_os_str().to_owned();
    name.push(".needs-verify.json");
    PathBuf::from(name)
}

fn lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn load(path: &Path) -> std::io::Result<Queue> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Queue::default());
        }
        Err(error) => return Err(error),
    };
    serde_json::from_slice(&bytes)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

fn save(path: &Path, queue: &Queue) -> std::io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = {
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        PathBuf::from(tmp)
    };
    let payload = serde_json::to_vec_pretty(queue)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    std::fs::write(&tmp, payload)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn with_queue(ledger: &Path, mutate: impl FnOnce(&mut Queue) -> bool) -> std::io::Result<()> {
    let _guard = lock();
    let path = path_for(ledger);
    let mut queue = load(&path)?;
    if mutate(&mut queue) {
        queue.version = VERSION;
        save(&path, &queue)?;
    }
    Ok(())
}

fn is_unsettled(position: &PairPosition) -> bool {
    ended_position(position) && position.realized_source.is_none()
}

fn retain_live(queue: &mut Queue) {
    queue
        .positions
        .retain(|_, entry| entry.unsettled || entry.reconciliation.is_some());
}

/// 名单是否已经做过一次历史未结算收录。文件不存在视为还没有。
pub fn is_bootstrapped(ledger: &Path) -> bool {
    let _guard = lock();
    load(&path_for(ledger))
        .map(|queue| queue.bootstrapped)
        .unwrap_or(false)
}

/// 当前需要手动核实的仓位 id。
pub fn position_ids(ledger: &Path) -> std::io::Result<BTreeSet<String>> {
    let _guard = lock();
    Ok(load(&path_for(ledger))?.positions.keys().cloned().collect())
}

/// 一笔仓位写入台账后调用：只更新这一笔，不读整本台账。
pub fn note_position(ledger: &Path, position: &PairPosition) -> std::io::Result<()> {
    let unsettled = is_unsettled(position);
    let id = position.id.clone();
    with_queue(ledger, |queue| {
        let Some(entry) = queue.positions.get(&id) else {
            if !unsettled {
                return false;
            }
            queue.positions.insert(
                id,
                Entry {
                    unsettled: true,
                    reconciliation: None,
                },
            );
            return true;
        };
        if entry.unsettled == unsettled {
            return false;
        }
        if let Some(entry) = queue.positions.get_mut(&id) {
            entry.unsettled = unsettled;
        }
        retain_live(queue);
        true
    })
}

/// 用一次已经读好的回放收录历史未结算（仅第一次），并按本次对账受牵连的敞口仓位增删。
pub fn sync_from_replay(
    ledger: &Path,
    replayed: &Replayed,
    implicated: &BTreeMap<String, String>,
) -> std::io::Result<()> {
    with_queue(ledger, |queue| {
        let mut dirty = false;
        if !queue.bootstrapped {
            for position in replayed.positions.values() {
                if !is_unsettled(position) {
                    continue;
                }
                let entry = queue.positions.entry(position.id.clone()).or_default();
                if !entry.unsettled {
                    entry.unsettled = true;
                }
            }
            queue.bootstrapped = true;
            dirty = true;
        }
        let mut seen = BTreeSet::new();
        for (id, why) in implicated {
            seen.insert(id.clone());
            let entry = queue.positions.entry(id.clone()).or_default();
            if entry.reconciliation.as_deref() != Some(why.as_str()) {
                entry.reconciliation = Some(why.clone());
                dirty = true;
            }
        }
        let stale: Vec<String> = queue
            .positions
            .iter()
            .filter(|(id, entry)| entry.reconciliation.is_some() && !seen.contains(*id))
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            if let Some(entry) = queue.positions.get_mut(&id) {
                entry.reconciliation = None;
                dirty = true;
            }
        }
        let before = queue.positions.len();
        retain_live(queue);
        dirty || queue.positions.len() != before
    })
}

/// 按最新回放去掉已经补齐、或台账里已经没有的名单项。不新收未在名单里的仓位。
pub fn refresh(ledger: &Path, replayed: &Replayed, ids: &BTreeSet<String>) -> std::io::Result<()> {
    with_queue(ledger, |queue| {
        let mut dirty = false;
        for id in ids {
            let Some(position) = replayed.positions.get(id) else {
                if queue.positions.remove(id).is_some() {
                    dirty = true;
                }
                continue;
            };
            let unsettled = is_unsettled(position);
            match queue.positions.get_mut(id) {
                Some(entry) if entry.unsettled != unsettled => {
                    entry.unsettled = unsettled;
                    dirty = true;
                }
                Some(_) => {}
                None => {}
            }
        }
        let before = queue.positions.len();
        retain_live(queue);
        dirty || queue.positions.len() != before
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PositionStatus, RealizedSource, Strategy};
    use arb_core::Symbol;
    use chrono::Utc;
    use rust_decimal::Decimal;

    fn position(id: &str, status: PositionStatus, settled: bool) -> PairPosition {
        PairPosition {
            id: id.into(),
            symbol: Symbol::perp("ETH", "USDT"),
            strategy: Strategy::Funding,
            long: None,
            short: None,
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status,
            opened_at: Utc::now(),
            closed_at: Some(Utc::now()),
            note: None,
            margin_mode: crate::MarginMode::Isolated,
            leverage: None,
            rules: Default::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: settled.then_some(RealizedSource::Executor),
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            origin: None,
        }
    }

    fn replay(positions: Vec<PairPosition>) -> Replayed {
        let mut replayed = Replayed::default();
        for position in positions {
            replayed.positions.insert(position.id.clone(), position);
        }
        replayed
    }

    #[test]
    fn the_queue_seeds_once_then_only_tracks_writes_and_dirty_positions() {
        let dir = std::env::temp_dir().join(format!("needs-verify-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let ledger = dir.join("arb-live-ledger.jsonl");
        let open = position("open", PositionStatus::Open, false);
        let pending = position("pending", PositionStatus::Unwound, false);
        let done = position("done", PositionStatus::Closed, true);
        let replayed = replay(vec![open.clone(), pending.clone(), done.clone()]);
        sync_from_replay(&ledger, &replayed, &BTreeMap::new()).unwrap();
        assert!(is_bootstrapped(&ledger));
        assert_eq!(
            position_ids(&ledger).unwrap(),
            BTreeSet::from(["pending".to_string()])
        );
        let later = replay(vec![
            open,
            pending.clone(),
            done,
            position("later", PositionStatus::Unwound, false),
        ]);
        sync_from_replay(&ledger, &later, &BTreeMap::new()).unwrap();
        assert!(
            !position_ids(&ledger).unwrap().contains("later"),
            "收录完成后不再把整本台账里的新未结算扫进来"
        );
        note_position(&ledger, &position("later", PositionStatus::Unwound, false)).unwrap();
        assert!(position_ids(&ledger).unwrap().contains("later"));
        note_position(&ledger, &position("later", PositionStatus::Unwound, true)).unwrap();
        assert!(!position_ids(&ledger).unwrap().contains("later"));

        let mut implicated = BTreeMap::new();
        implicated.insert("open".into(), "挂单对不上".into());
        sync_from_replay(&ledger, &later, &implicated).unwrap();
        assert!(position_ids(&ledger).unwrap().contains("open"));
        assert!(position_ids(&ledger).unwrap().contains("pending"));
        sync_from_replay(&ledger, &later, &BTreeMap::new()).unwrap();
        assert!(!position_ids(&ledger).unwrap().contains("open"));
        assert!(position_ids(&ledger).unwrap().contains("pending"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
