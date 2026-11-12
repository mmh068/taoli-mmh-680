//! 北京时间 08:00 运行统计：过去 24 小时成交订单、已实现盈亏、累计已实现盈亏。

use std::sync::Arc;
use std::time::Duration;

use arb_core::Decimal;
use arb_exec::Replayed;
use arb_exec::desk;
use chrono::{DateTime, FixedOffset, Utc};
use tracing::{info, warn};

use crate::AppState;

/// 北京时间（UTC+8，无夏令时）。
fn beijing() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).expect("UTC+8")
}

/// 每天北京时间 08:00 发一条统计到 Telegram 和 Discord。
pub fn spawn(state: &Arc<AppState>) {
    if !state.alerts.enabled() {
        return;
    }
    let state = Arc::clone(state);
    tokio::spawn(async move {
        loop {
            let wait = until_next_beijing_hour(8, Utc::now());
            info!(
                wait_sec = wait.as_secs(),
                "运行统计将在下一个北京时间 08:00 发送"
            );
            tokio::time::sleep(wait).await;
            let text = digest(&state).await;
            if state.alerts.notify_digest(text) {
                info!("已发送运行统计");
            } else {
                warn!("运行统计没发出去（未配置告警或正在静音）");
            }
        }
    });
}

/// 距下一个北京时间 `hour:00:00` 还有多久。正好落在整点则等下一轮。
pub fn until_next_beijing_hour(hour: u32, now: DateTime<Utc>) -> Duration {
    let tz = beijing();
    let local = now.with_timezone(&tz);
    let today = local
        .date_naive()
        .and_hms_opt(hour, 0, 0)
        .expect("valid hms")
        .and_local_timezone(tz)
        .single()
        .expect("UTC+8 无夏令时歧义");
    let next = if local < today {
        today
    } else {
        today + chrono::Duration::days(1)
    };
    (next.with_timezone(&Utc) - now)
        .to_std()
        .unwrap_or(Duration::from_secs(1))
        .max(Duration::from_secs(1))
}

async fn digest(state: &AppState) -> String {
    let now = Utc::now();
    let since = now - chrono::Duration::hours(24);
    match state.trade.stats_replay().await {
        Ok((replayed, path)) => format_digest(&path, &replayed, now, since),
        Err(reason) => format!(
            "📊 运行统计（截至北京时间 {}）\n读不了台账：{reason}",
            now.with_timezone(&beijing()).format("%Y-%m-%d %H:%M")
        ),
    }
}

pub fn format_digest(
    ledger_path: &std::path::Path,
    replayed: &Replayed,
    now: DateTime<Utc>,
    since: DateTime<Utc>,
) -> String {
    let orders = desk::filled_orders_since(replayed, since);
    let last24 = desk::realized_since(replayed, since);
    let total = desk::cumulative_totals(ledger_path, replayed);
    format!(
        "📊 运行统计（过去 24 小时，截至北京时间 {}）\n成交订单：{} 笔\n过去 24 小时已实现盈亏：{}\n累计已实现盈亏：{}（已结算 {} / 结束 {} 笔）",
        now.with_timezone(&beijing()).format("%Y-%m-%d %H:%M"),
        orders,
        money(last24.net_usdt),
        money(total.net_usdt),
        total.settled,
        total.closed,
    )
}

fn money(value: Decimal) -> String {
    let value = value.round_dp(2).normalize();
    if value > Decimal::ZERO {
        format!("+{value} USDT")
    } else {
        format!("{value} USDT")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn waits_until_the_next_beijing_8() {
        // 北京时间 07:59 = UTC 23:59（前一天）
        let now = Utc.with_ymd_and_hms(2026, 10, 15, 23, 59, 0).unwrap();
        assert_eq!(until_next_beijing_hour(8, now), Duration::from_secs(60));
        // 正好北京时间 08:00 = UTC 00:00，应等到明天
        let on_the_hour = Utc.with_ymd_and_hms(2026, 10, 16, 0, 0, 0).unwrap();
        assert_eq!(
            until_next_beijing_hour(8, on_the_hour),
            Duration::from_secs(24 * 3600)
        );
        let just_after = Utc.with_ymd_and_hms(2026, 10, 16, 0, 0, 1).unwrap();
        let wait = until_next_beijing_hour(8, just_after);
        assert!(wait > Duration::from_secs(23 * 3600));
        assert!(wait < Duration::from_secs(24 * 3600));
    }
}
