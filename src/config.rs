use exchanges::Ticker;
use exchanges::adapter::{Event, MarketType};
use futures::Stream;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::pin::Pin;
use std::str::FromStr;

#[derive(Debug, Deserialize, Default)]
pub struct Config {
    #[serde(flatten)]
    pub exchanges: HashMap<String, ExchangeConfig>,
}

#[derive(Debug, Deserialize, Default)]
pub struct ExchangeConfig {
    #[serde(flatten)]
    pub markets: HashMap<String, MarketConfig>,
}

#[derive(Debug, Deserialize, Default)]
pub struct MarketConfig {
    #[serde(default)]
    pub tickers: Vec<String>,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: Config = toml::de::from_str(&content)?;
        Ok(config)
    }
}

pub fn setup_exchange_streams<F, S>(
    exchange_display_name: &str,
    exchange_config: &ExchangeConfig,
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
