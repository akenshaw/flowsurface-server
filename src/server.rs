use std::env;

use exchanges::Trade;
use exchanges::adapter::StreamType;
use serde::{Deserialize, Serialize};

use clickhouse::sql::Identifier;
use clickhouse::{Client, Row, error::Result};

fn read_env_var(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("{key} env variable should be set"))
}

pub fn create_client() -> Client {
    Client::default()
        .with_url(read_env_var("CLICKHOUSE_URL"))
        .with_user(read_env_var("CLICKHOUSE_USER"))
        .with_password(read_env_var("CLICKHOUSE_PASSWORD"))
        .with_option("async_insert", "1")
        .with_option("wait_for_async_insert", "0")
}

const TABLE_NAME: &str = "market_trades";
const TABLE_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS ? (
        timestamp DateTime64(3),
        exchange String,
        symbol String,
        price Float64,
        quantity Float64,
        is_buyer_maker UInt8
    )
    ENGINE = MergeTree
    ORDER BY (exchange, symbol, timestamp)
";

#[derive(Debug, Serialize, Deserialize, Row)]
pub struct TradeData {
    timestamp: u64,
    exchange: String,
    symbol: String,
    price: f64,
    quantity: f64,
    is_buyer_maker: bool,
}

pub async fn insert_trades(
    client: &mut Client,
    stream_type: StreamType,
    trades: &[Trade],
) -> Result<()> {
    client
        .query(TABLE_SCHEMA)
        .bind(Identifier(TABLE_NAME))
        .execute()
        .await?;

    let mut insert = client.insert(TABLE_NAME)?;

    if let StreamType::DepthAndTrades { exchange, ticker } = stream_type {
        let symbol = ticker.to_string();
        let exchange = exchange.to_string();

        for trade in trades {
            insert
                .write(&TradeData {
                    timestamp: trade.time,
                    exchange: exchange.clone(),
                    symbol: symbol.clone(),
                    price: trade.price as f64,
                    quantity: trade.qty as f64,
                    is_buyer_maker: trade.is_sell,
                })
                .await?;
        }
    }

    insert.end().await?;

    Ok(())
}
