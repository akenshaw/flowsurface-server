use std::str::FromStr;

use anyhow::Result;
use config::Config;
use exchanges::Ticker;
use exchanges::adapter::{Event, MarketType};
use exchanges::adapter::{binance, bybit};
use futures::{Stream, StreamExt};
use tokio::signal;

mod config;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    println!("Starting Flowsurface server...");

    let config: Config =
        Config::from_file("./config/tickers.toml").expect("Failed to load config file");

    println!("{:?}", &config);

    let market_type = MarketType::from_str(&config.market_type)?;

    let book_tickers: Vec<Ticker> = config
        .tickers
        .iter()
        .map(|ticker| Ticker::new(ticker, market_type))
        .collect();

    let stream = bybit::connect_market_stream(book_tickers, market_type);
    let pinned_stream = Box::pin(stream);

    tokio::select! {
        _ = process_stream(pinned_stream) => {},
        _ = signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down");
        }
    }

    Ok(())
}

async fn process_stream<S>(mut stream: S)
where
    S: Stream<Item = Event> + Unpin,
{
    while let Some(event) = stream.next().await {
        match &event {
            Event::Connected(exchange, _) => {
                println!("Connected to {}", exchange);
            }
            Event::Disconnected(exchange, reason) => {
                println!("Disconnected from {}: {}", exchange, reason);
            }
            Event::DepthReceived(stream_type, time, depth, trades) => {
                println!(
                    "{} -- [{}] Received depth update: {} bid/ask levels, {} trades",
                    stream_type,
                    time,
                    depth.bids.len() + depth.asks.len(),
                    trades.len()
                );
            }
            Event::KlineReceived(stream_type, kline) => {
                println!("{} -- Received kline update: {}", stream_type, kline.close,);
            }
        }
    }
}
