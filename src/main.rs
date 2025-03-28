use anyhow::Result;
use exchanges::Ticker;
use exchanges::adapter::bybit::connect_market_stream;
use exchanges::adapter::{Event, MarketType};
use futures::{Stream, StreamExt};
use tokio::signal;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    println!("Starting Flowsurface server...");

    let ticker = Ticker::new("BTCUSDT", MarketType::Spot);
    println!("Connecting to market stream for {}", ticker.get_string().0);

    let stream = connect_market_stream(ticker);
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
            _ => {
                println!("Received event: {:?}", event);
            }
        }
    }
}
