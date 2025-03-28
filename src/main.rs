use config::Config;
use exchanges::Ticker;
use exchanges::adapter::{Event, MarketType};
use exchanges::adapter::{binance, bybit};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::str::FromStr;
use tokio::signal;

mod config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    println!("Starting Flowsurface server...");

    let config: Config =
        Config::from_file("./config/tickers.toml").expect("Failed to load config file");

    println!("{:?}", &config);

    let mut streams = Vec::new();

    // Process all configured exchanges
    for (exchange_name, exchange_config) in &config.exchanges {
        match exchange_name.as_str() {
            "binance" => setup_exchange_streams(
                "Binance",
                exchange_config,
                &mut streams,
                binance::connect_market_stream,
            )?,
            "bybit" => setup_exchange_streams(
                "Bybit",
                exchange_config,
                &mut streams,
                bybit::connect_market_stream,
            )?,
            _ => println!("Unknown exchange: {}", exchange_name),
        }
    }

    let merged_stream = futures::stream::select_all(streams);

    tokio::select! {
        _ = process_stream(merged_stream) => {},
        _ = signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down");
        }
    }

    Ok(())
}

fn setup_exchange_streams<F, S>(
    exchange_display_name: &str,
    exchange_config: &config::ExchangeConfig,
    streams: &mut Vec<Pin<Box<dyn Stream<Item = Event> + Send>>>,
    connect_fn: F,
) -> anyhow::Result<()>
where
    S: Stream<Item = Event> + Send + 'static,
    F: Fn(Vec<Ticker>, MarketType) -> S,
{
    for (market_type_str, market_config) in &exchange_config.markets {
        if !market_config.tickers.is_empty() {
            let market_type = MarketType::from_str(market_type_str)?;

            let book_tickers: Vec<Ticker> = market_config
                .tickers
                .iter()
                .map(|ticker| Ticker::new(ticker, market_type))
                .collect();

            let stream = connect_fn(book_tickers, market_type);
            streams.push(Box::pin(stream) as Pin<Box<dyn Stream<Item = Event> + Send>>);

            println!(
                "Added {} {} stream with {} tickers",
                exchange_display_name,
                market_type_str,
                market_config.tickers.len()
            );
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
                //println!(
                //    "{} -- [{}] Received depth update: {} bid/ask levels, {} trades",
                //    stream_type,
                //    time,
                //    depth.bids.len() + depth.asks.len(),
                //    trades.len()
                //);

                println!("{stream_type}")
            }
            Event::KlineReceived(stream_type, kline) => {
                println!("{} -- Received kline update: {}", stream_type, kline.close,);
            }
        }
    }
}
