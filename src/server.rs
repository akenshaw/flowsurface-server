use clickhouse::sql::Identifier;
use clickhouse::{Client, Row, error::Result};
use exchanges::Trade;
use exchanges::adapter::StreamType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

fn read_env_var(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{key} env variable should be set"))
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
struct TradeData {
    timestamp: u64,
    exchange: String,
    symbol: String,
    price: f64,
    quantity: f64,
    is_buyer_maker: bool,
}

async fn get_inserter(client: &mut Client) -> Result<clickhouse::insert::Insert<TradeData>> {
    client
        .query(TABLE_SCHEMA)
        .bind(Identifier(TABLE_NAME))
        .execute()
        .await?;

    client.insert(TABLE_NAME)
}

async fn write_buffer(
    inserter: &mut clickhouse::insert::Insert<TradeData>,
    stream_type: &StreamType,
    trades: &[Trade],
) -> Result<()> {
    if let StreamType::DepthAndTrades { exchange, ticker } = stream_type {
        let symbol = ticker.to_string();
        let exchange = exchange.to_string();

        for trade in trades {
            inserter
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

    Ok(())
}

pub async fn flush_trades_buffer(
    client: &mut clickhouse::Client,
    trades_buffer: &mut HashMap<StreamType, Vec<Trade>>,
) -> anyhow::Result<()> {
    let mut inserter = get_inserter(client).await?;

    for (stream_type, trades) in trades_buffer.iter_mut() {
        if !trades.is_empty() {
            write_buffer(&mut inserter, stream_type, trades).await?;
            trades.clear();
        }
    }

    inserter.end().await?;
    Ok(())
}
