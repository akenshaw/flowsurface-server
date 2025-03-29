use config::{Config, setup_exchange_streams};
use dotenv::dotenv;
use exchanges::{
    Trade,
    adapter::{Event, StreamType, binance, bybit},
};
use futures::Stream;
use futures::StreamExt;
use std::{collections::HashMap, time::Duration};
use tokio::{sync::mpsc, time::interval};

mod config;
mod server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    dotenv().ok();

    let config: Config =
        Config::from_file("./config/tickers.toml").expect("Failed to load config file");

    let mut streams = Vec::new();

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

    let mut client = server::create_client();

    let mut tick_to_write = interval(Duration::from_secs(2));

    let (tx, mut rx) = mpsc::channel::<(StreamType, Box<[Trade]>)>(100);

    tokio::select! {
        _ = process_stream(merged_stream, tx) => {},
        _ = async move {
            let mut trades_buffer: HashMap<StreamType, Vec<Trade>> = HashMap::new();

            loop {
                tokio::select! {
                    Some((stream_type, trades)) = rx.recv() => {
                        trades_buffer
                            .entry(stream_type)
                            .or_insert_with(Vec::new)
                            .extend(trades);
                    },
                    _ = tick_to_write.tick() => {
                        for (stream_type, trades) in trades_buffer.iter_mut() {
                            if !trades.is_empty() {
                                if let Err(e) = server::insert_trades(&mut client, *stream_type, trades).await {
                                    eprintln!("Error inserting trades: {}", e);
                                }
                                trades.clear();
                            }
                        }
                    }
                }
            }
        } => {},
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down");
        }
    }

    Ok(())
}

async fn process_stream<S>(mut stream: S, tx: mpsc::Sender<(StreamType, Box<[Trade]>)>)
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
            Event::DepthReceived(stream_type, _, _, trades) => {
                if !trades.is_empty() {
                    if let Err(e) = tx.send((*stream_type, trades.clone())).await {
                        eprintln!("Failed to send trades: {}", e);
                    }
                }
            }
            Event::KlineReceived(stream_type, kline) => {
                println!("{} -- Received kline update: {}", stream_type, kline.close,);
            }
        }
    }
}
