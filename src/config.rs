use serde::Deserialize;
use std::collections::HashMap;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub exchanges: HashMap<String, ExchangeConfig>,
}

#[derive(Debug, Deserialize, Default)]
pub struct ExchangeConfig {
    #[serde(default)]
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
