use config::{Config, setup_exchange_streams};
use dotenv::dotenv;
use exchanges::{
    Trade,
    adapter::{Event, StreamType, binance, bybit},
};
use futures::{Stream, StreamExt};
use std::{collections::HashMap, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    time::interval,
};

mod config;
mod server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    let config: Config =
        Config::from_file("./config/tickers.toml").expect("Failed to load config file");

    let mut streams = Vec::new();

    for (exchange_name, exchange_config) in &config.exchanges {
        match exchange_name.as_str() {
            "binance" => setup_exchange_streams(
                exchange_config,
                &mut streams,
                binance::connect_market_stream,
            )?,
            "bybit" => {
                setup_exchange_streams(exchange_config, &mut streams, bybit::connect_market_stream)?
            }
            _ => println!("Unknown exchange: {}", exchange_name),
        }
    }

    let client = server::create_client();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (buffer_tx, buffer_rx) = mpsc::channel::<(StreamType, Box<[Trade]>)>(100);

    let merged_stream = futures::stream::select_all(streams);
    let buffer_handle = tokio::spawn(process_trades_buffer(buffer_rx, client, shutdown_rx));

    tokio::select! {
        _ = process_stream(merged_stream, buffer_tx) => {},
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down");
            let _ = shutdown_tx.send(());
        }
    }

    match buffer_handle.await {
        Ok(_) => {
            println!("Buffer processing finished");
            Ok(())
        }
        Err(e) => {
            eprintln!("Error in buffer processing: {}", e);
            Err(e.into())
        }
    }
}

async fn process_trades_buffer(
    mut rx: mpsc::Receiver<(StreamType, Box<[Trade]>)>,
    mut client: clickhouse::Client,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let mut trades_buffer: HashMap<StreamType, Vec<Trade>> = HashMap::new();
    let mut flush_timer = interval(Duration::from_secs(3));

    loop {
        tokio::select! {
            Some((stream_type, trades)) = rx.recv() => {
                trades_buffer
                    .entry(stream_type)
                    .or_default()
                    .extend(trades);
            },
            _ = flush_timer.tick() => {
                if let Err(e) = server::flush_trades_buffer(&mut client, &mut trades_buffer).await {
                    eprintln!("Error flushing trades buffer: {}", e);
                }
            }
            _ = &mut shutdown_rx => {
                println!("Buffer task received shutdown signal");
                if let Err(e) = server::flush_trades_buffer(&mut client, &mut trades_buffer).await {
                    eprintln!("Error flushing trades buffer during shutdown: {}", e);
                }
                break;
            }
        }
    }
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
