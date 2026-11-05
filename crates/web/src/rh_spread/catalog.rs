//! 各家场所的市场列表（REST，5 分钟刷新一次）：订阅名、吃单费率、类别、交易时段。纯解析函数单测覆盖。

use std::collections::{BTreeMap, BTreeSet};

use arb_core::{Decimal, Venue};
use serde_json::Value;

use super::pairs::VenueMarket;

/// Lighter 前端发布的分类表。公开 REST 没有 RWA/Pre-IPO 字段，页面分组读的是这份清单。
pub(super) const LIGHTER_TOKEN_CATEGORIES_URL: &str =
    "https://app.lighter.xyz/assets/dist-Cw2idzK3.js";

/// 当前前端模块里的分类表版本。模块换名时拒绝旧缓存，不能猜一份新名单。
pub(super) const LIGHTER_TOKEN_TABLE_MARKER: &str = "SENTRY_RELEASE={id:`18801fb6901f1a61692d4bd530eeb2fdb069b98f`}";

/// Arcus `/v1/markets`：在线永续，`marketDisplayName == "{base}-USD"`。费率是基础档（上限）。
pub fn arcus(markets: &Value, taker: Decimal) -> anyhow::Result<BTreeMap<String, VenueMarket>> {
    anyhow::ensure!(
        (Decimal::ZERO..Decimal::new(1, 2)).contains(&taker),
        "Arcus 吃单费率 {taker} 超出合理范围"
    );
    let rows = markets
        .get("markets")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("Arcus markets 格式不对"))?;
    let mut out = BTreeMap::new();
    for m in rows {
        if m.get("status").and_then(Value::as_str) != Some("ONLINE")
            || m.get("type").and_then(Value::as_str) != Some("PERPETUAL")
        {
            continue;
        }
        let (Some(base), Some(name)) = (
            m.get("baseAsset").and_then(Value::as_str),
            m.get("marketDisplayName").and_then(Value::as_str),
        ) else {
            continue;
        };
        if name != format!("{base}-USD") {
            continue;
        }
        out.insert(
            base.to_ascii_uppercase(),
            VenueMarket {
                key: name.to_string(),
                taker_fee: Some(taker),
                category: Some(
                    m.get("category")
                        .and_then(Value::as_str)
                        .unwrap_or("CRYPTO")
                        .to_string(),
                ),
                outside_rth: m.get("isOutsideRth").and_then(Value::as_bool),
            },
        );
    }
    anyhow::ensure!(!out.is_empty(), "Arcus 没有在线永续");
    Ok(out)
}

/// Lighter RH `/api/v1/orderBookDetails`：活跃永续。`taker_fee` 与扫描器同口径直接当小数（实测 `"0.0000"`）；
/// 没报费率的不当 0，记为不知道。
pub fn lighter(details: &Value) -> anyhow::Result<BTreeMap<String, VenueMarket>> {
    let rows = details
        .get("order_book_details")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("Lighter RH orderBookDetails 格式不对"))?;
    let mut out = BTreeMap::new();
    for r in rows {
        if r.get("status").and_then(Value::as_str) != Some("active")
            || r.get("market_type")
                .and_then(Value::as_str)
                .is_some_and(|t| t != "perp")
        {
            continue;
        }
        let (Some(symbol), Some(id)) = (
            r.get("symbol").and_then(Value::as_str),
            r.get("market_id").and_then(Value::as_i64),
        ) else {
            continue;
        };
        out.insert(
            symbol.to_ascii_uppercase(),
            VenueMarket {
                key: id.to_string(),
                taker_fee: r
                    .get("taker_fee")
                    .and_then(Value::as_str)
                    .and_then(arb_core::parse_decimal),
                category: None,
                outside_rth: None,
            },
        );
    }
    anyhow::ensure!(!out.is_empty(), "Lighter RH 没有活跃永续");
    Ok(out)
}

/// Lighter 前端分类表里，`asset_type = RWA` 且 `categories` 含 `PRE_IPO` 的全部币名。
///
/// 只认这两个显式字段。模块版本不符或表被截断时报错，不把残缺名单当成完整名单。
pub fn lighter_rh_preipo_bases(script: &str) -> anyhow::Result<BTreeSet<String>> {
    let table_at = script
        .find("tokens:[")
        .ok_or_else(|| anyhow::anyhow!("Lighter 分类表没有 tokens"))?;
    let table = &script[table_at..];
    anyhow::ensure!(
        script.contains(LIGHTER_TOKEN_TABLE_MARKER) && table.contains("],Kn="),
        "Lighter 分类表版本已变或内容不完整"
    );
    let mut out = BTreeSet::new();
    for row in table.split("{symbol:`").skip(1) {
        let Some((symbol, rest)) = row.split_once('`') else {
            continue;
        };
        let asset_type = field(rest, "asset_type")?;
        let categories = rest
            .split_once("categories:[")
            .and_then(|(_, value)| value.split_once(']'))
            .map(|(value, _)| value)
            .ok_or_else(|| anyhow::anyhow!("Lighter 分类表缺 categories"))?;
        if asset_type == "RWA"
            && categories
                .split(',')
                .any(|category| category.trim().trim_matches('`') == "PRE_IPO")
        {
            out.insert(symbol.to_ascii_uppercase());
        }
    }
    anyhow::ensure!(!out.is_empty(), "Lighter RH 的 RWA/Pre-IPO 名单为空");
    Ok(out)
}

fn field<'a>(row: &'a str, name: &str) -> anyhow::Result<&'a str> {
    row.split_once(&format!("{name}:`"))
        .and_then(|(_, value)| value.split_once('`'))
        .map(|(value, _)| value)
        .ok_or_else(|| anyhow::anyhow!("Lighter 分类表缺 {name}"))
}

/// Hyperliquid `perpCategories` 里标为 `preipo` 的名称，按各 dex 转成监控使用的 base。
///
/// 主 dex 的 Pre-IPO 直接用原名；xyz/io 只收对应前缀。别的 dex 不进价差监控，不能因为它们的同名合约被连带排除。
pub fn hyperliquid_preipo_bases(rows: &Value) -> anyhow::Result<BTreeSet<String>> {
    let rows = rows
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Hyperliquid perpCategories 格式不对"))?;
    let mut out = BTreeSet::new();
    for row in rows {
        let pair = row
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Hyperliquid perpCategories 行格式不对"))?;
        let (Some(name), Some(category)) = (
            pair.first().and_then(Value::as_str),
            pair.get(1).and_then(Value::as_str),
        ) else {
            continue;
        };
        if !category.eq_ignore_ascii_case("preipo") {
            continue;
        }
        let venue = if name.starts_with("xyz:") {
            Venue::HyperliquidXyz
        } else if name.starts_with("io:") {
            Venue::HyperliquidIo
        } else if !name.contains(':') {
            Venue::Hyperliquid
        } else {
            continue;
        };
        if let Some(base) = arb_venues::hyperliquid::base_for(venue, name) {
            out.insert(base);
        }
    }
    Ok(out)
}

/// Hyperliquid `meta`（主 dex 或 `dex` = xyz / io）：未下架的合约。费率按
/// [`arb_venues::hyperliquid::taker_fee_for`]（基础档上限、HIP-3 倍数、growth mode、io 的 Entropy 返佣）。
pub fn hyperliquid(
    venue: Venue,
    meta: &Value,
    rebate: Decimal,
) -> anyhow::Result<BTreeMap<String, VenueMarket>> {
    let rows = meta
        .get("universe")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("{venue} meta 格式不对"))?;
    let mut out = BTreeMap::new();
    for asset in rows {
        if asset
            .get("isDelisted")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            continue;
        }
        let Some(name) = asset.get("name").and_then(Value::as_str) else {
            continue;
        };
        let Some(base) = arb_venues::hyperliquid::base_for(venue, name) else {
            continue;
        };
        let fee = arb_venues::hyperliquid::taker_fee_for(
            venue,
            asset.get("deployerFeeScale").and_then(Value::as_str),
            asset.get("growthMode").and_then(Value::as_str),
            rebate,
        );
        out.insert(
            base,
            VenueMarket {
                key: name.to_string(),
                taker_fee: fee,
                category: None,
                outside_rth: None,
            },
        );
    }
    anyhow::ensure!(!out.is_empty(), "{venue} 没有可交易的永续");
    Ok(out)
}
