use std::collections::HashMap;

use crate::{Kline, OpenInterest, TickerInfo, TickerStats, Trade, depth::Depth};

use super::{Ticker, Timeframe};
use serde::{Deserialize, Serialize};

pub mod binance;
pub mod bybit;

use futures::channel::mpsc;
use futures::stream::{self, Stream, StreamExt};

use std::future::Future;

pub fn channel<T, F>(size: usize, f: impl FnOnce(mpsc::Sender<T>) -> F) -> impl Stream<Item = T>
where
    F: Future<Output = ()>,
{
    let (sender, receiver) = mpsc::channel(size);

    let runner = stream::once(f(sender)).filter_map(|_| async { None });

    stream::select(receiver, runner)
}

#[derive(thiserror::Error, Debug)]
pub enum StreamError {
    #[error("Fetchrror: {0}")]
    FetchError(#[from] reqwest::Error),
    #[error("Parsing error: {0}")]
    ParseError(String),
    #[error("Stream error: {0}")]
    WebsocketError(String),
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    #[error("{0}")]
    UnknownError(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum MarketType {
    Spot,
    LinearPerps,
    InversePerps,
}

impl std::str::FromStr for MarketType {
    type Err = StreamError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "spot" => Ok(MarketType::Spot),
            "linear_perps" => Ok(MarketType::LinearPerps),
            "inverse_perps" => Ok(MarketType::InversePerps),
            _ => Err(StreamError::InvalidRequest(
                "Invalid market type".to_string(),
            )),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum StreamType {
    Kline {
        exchange: Exchange,
        ticker: Ticker,
        timeframe: Timeframe,
    },
    DepthAndTrades {
        exchange: Exchange,
        ticker: Ticker,
    },
    None,
}

impl std::fmt::Display for StreamType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamType::Kline {
                exchange,
                ticker,
                timeframe,
            } => write!(f, "{} {} {}", exchange, ticker.get_string().0, timeframe),
            StreamType::DepthAndTrades { exchange, ticker } => {
                write!(f, "{} {}", exchange, ticker.get_string().0)
            }
            StreamType::None => write!(f, "None"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum Exchange {
    BinanceLinear,
    BinanceInverse,
    BinanceSpot,
    BybitLinear,
    BybitInverse,
    BybitSpot,
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Exchange::BinanceLinear => "Binance Linear",
                Exchange::BinanceInverse => "Binance Inverse",
                Exchange::BinanceSpot => "Binance Spot",
                Exchange::BybitLinear => "Bybit Linear",
                Exchange::BybitInverse => "Bybit Inverse",
                Exchange::BybitSpot => "Bybit Spot",
            }
        )
    }
}

impl Exchange {
    pub const ALL: [Exchange; 6] = [
        Exchange::BinanceLinear,
        Exchange::BinanceInverse,
        Exchange::BinanceSpot,
        Exchange::BybitLinear,
        Exchange::BybitInverse,
        Exchange::BybitSpot,
    ];

    pub fn get_market_type(&self) -> MarketType {
        match self {
            Exchange::BinanceLinear | Exchange::BybitLinear => MarketType::LinearPerps,
            Exchange::BinanceInverse | Exchange::BybitInverse => MarketType::InversePerps,
            Exchange::BinanceSpot | Exchange::BybitSpot => MarketType::Spot,
        }
    }

    pub fn is_inverse(&self) -> bool {
        matches!(self, Exchange::BinanceInverse | Exchange::BybitInverse)
    }

    pub fn is_linear(&self) -> bool {
        matches!(self, Exchange::BinanceLinear | Exchange::BybitLinear)
    }

    pub fn is_spot(&self) -> bool {
        matches!(self, Exchange::BinanceSpot | Exchange::BybitSpot)
    }
}

#[derive(Debug, Clone)]
pub struct Connection;

#[derive(Debug, Clone)]
pub enum Event {
    Connected(Exchange, Connection),
    Disconnected(Exchange, String),
    DepthReceived(StreamType, u64, Depth, Box<[Trade]>),
    KlineReceived(StreamType, Kline),
}

#[derive(Debug, Clone, Hash)]
pub struct StreamConfig<I> {
    pub id: I,
    pub market_type: MarketType,
}

impl<I> StreamConfig<I> {
    pub fn new(id: I, exchange: Exchange) -> Self {
        let market_type = exchange.get_market_type();

        Self { id, market_type }
    }
}

pub async fn fetch_ticker_info(
    exchange: Exchange,
) -> Result<HashMap<Ticker, Option<TickerInfo>>, StreamError> {
    let market_type = exchange.get_market_type();

    match exchange {
        Exchange::BinanceLinear | Exchange::BinanceInverse | Exchange::BinanceSpot => {
            binance::fetch_ticksize(market_type).await
        }
        Exchange::BybitLinear | Exchange::BybitInverse | Exchange::BybitSpot => {
            bybit::fetch_ticksize(market_type).await
        }
    }
}

pub async fn fetch_ticker_prices(
    exchange: Exchange,
) -> Result<HashMap<Ticker, TickerStats>, StreamError> {
    let market_type = exchange.get_market_type();

    match exchange {
        Exchange::BinanceLinear | Exchange::BinanceInverse | Exchange::BinanceSpot => {
            binance::fetch_ticker_prices(market_type).await
        }
        Exchange::BybitLinear | Exchange::BybitInverse | Exchange::BybitSpot => {
            bybit::fetch_ticker_prices(market_type).await
        }
    }
}

pub async fn fetch_klines(
    exchange: Exchange,
    ticker: Ticker,
    timeframe: Timeframe,
    range: Option<(u64, u64)>,
) -> Result<Vec<Kline>, StreamError> {
    match exchange {
        Exchange::BinanceLinear | Exchange::BinanceInverse | Exchange::BinanceSpot => {
            binance::fetch_klines(ticker, timeframe, range).await
        }
        Exchange::BybitLinear | Exchange::BybitInverse | Exchange::BybitSpot => {
            bybit::fetch_klines(ticker, timeframe, range).await
        }
    }
}

pub async fn fetch_open_interest(
    exchange: Exchange,
    ticker: Ticker,
    timeframe: Timeframe,
    range: Option<(u64, u64)>,
) -> Result<Vec<OpenInterest>, StreamError> {
    match exchange {
        Exchange::BinanceLinear | Exchange::BinanceInverse => {
            binance::fetch_historical_oi(ticker, range, timeframe).await
        }
        Exchange::BybitLinear | Exchange::BybitInverse => {
            bybit::fetch_historical_oi(ticker, range, timeframe).await
        }
        _ => Err(StreamError::InvalidRequest("Invalid exchange".to_string())),
    }
}
