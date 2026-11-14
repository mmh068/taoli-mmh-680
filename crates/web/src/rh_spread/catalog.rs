//! 各家场所的市场列表（REST，5 分钟刷新一次）：订阅名、吃单费率、类别、交易时段。纯解析函数单测覆盖。

use std::collections::{BTreeMap, BTreeSet};

use arb_core::{Decimal, Venue};
use serde_json::Value;

use super::pairs::VenueMarket;

/// Lighter 首页。分类表在带哈希的前端包里，文件名每次发布都会变；写死就会 404，名单一直是空的。
pub(super) const LIGHTER_APP_URL: &str = "https://app.lighter.xyz/";

/// 前端包开头的发布标记。只认有这个标记的完整脚本，不把错误页或截断响应当成名单。
/// 发布号每次部署都会变，不能写死，否则换包之后名单会一直读不到。
pub(super) const LIGHTER_TOKEN_TABLE_MARKER: &str = "SENTRY_RELEASE={id:`";

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
                period_rate: None,
                interval_h: None,
                funding_at: None,
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
                period_rate: None,
                interval_h: None,
                funding_at: None,
            },
        );
    }
    anyhow::ensure!(!out.is_empty(), "Lighter RH 没有活跃永续");
    Ok(out)
}

/// 从首页里取当前分类表脚本。只接受唯一的 `/assets/dist-<hash>.js`，多份不同哈希或一份都没有就报错。
pub fn lighter_token_script_url(page: &str) -> anyhow::Result<String> {
    const PREFIX: &str = "/assets/dist-";
    let mut found = BTreeSet::new();
    let mut rest = page;
    while let Some(at) = rest.find(PREFIX) {
        let hash_at = at + PREFIX.len();
        let hash_end = rest[hash_at..]
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-')
            .map_or(rest.len(), |n| hash_at + n);
        if hash_end > hash_at && rest[hash_end..].starts_with(".js") {
            found.insert(rest[at..hash_end + 3].to_string());
        }
        rest = &rest[hash_at..];
    }
    let mut found = found.into_iter();
    let Some(path) = found.next() else {
        anyhow::bail!("Lighter 首页没有分类表地址");
    };
    anyhow::ensure!(
        found.next().is_none(),
        "Lighter 首页有多份分类表地址，不能猜"
    );
    Ok(format!("{}{path}", LIGHTER_APP_URL.trim_end_matches('/')))
}

/// Lighter 前端分类表里，`asset_type = RWA` 且 `categories` 含 `PRE_IPO` 的全部币名。
///
/// 只认这两个显式字段。脚本没有发布标记，或表被截断时报错，不把残缺名单当成完整名单。
///
/// 表后面的压缩变量名会变（曾经是 `],Kn=`，现在是 `]},Jn=`）。只要求 `tokens:[` 配平闭合，且闭合后文件还没有结束。
pub fn lighter_rh_preipo_bases(script: &str) -> anyhow::Result<BTreeSet<String>> {
    let table = tokens_table(script)?;
    anyhow::ensure!(
        script.contains(LIGHTER_TOKEN_TABLE_MARKER),
        "Lighter 分类表内容不完整"
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

/// `tokens:[...]` 的闭合范围。反引号里的括号不算。没闭合，或闭合后就是文件末尾，视为截断。
fn tokens_table(script: &str) -> anyhow::Result<&str> {
    let table_at = script
        .find("tokens:[")
        .ok_or_else(|| anyhow::anyhow!("Lighter 分类表没有 tokens"))?;
    let table = &script[table_at..];
    let start = table
        .find('[')
        .ok_or_else(|| anyhow::anyhow!("Lighter 分类表没有 tokens"))?;
    let mut depth = 0i32;
    let mut in_tick = false;
    for (index, byte) in table.bytes().enumerate().skip(start) {
        if byte == b'`' {
            in_tick = !in_tick;
            continue;
        }
        if in_tick {
            continue;
        }
        match byte {
            b'[' => depth += 1,
            b']' => {
                depth -= 1;
                if depth == 0 {
                    anyhow::ensure!(
                        index + 1 < table.len(),
                        "Lighter 分类表版本已变或内容不完整"
                    );
                    return Ok(&table[..=index]);
                }
            }
            _ => {}
        }
    }
    Err(anyhow::anyhow!("Lighter 分类表版本已变或内容不完整"))
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
                period_rate: None,
                interval_h: None,
                funding_at: None,
            },
        );
    }
    anyhow::ensure!(!out.is_empty(), "{venue} 没有可交易的永续");
    Ok(out)
}
