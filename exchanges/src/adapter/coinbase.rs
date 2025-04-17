use chrono::DateTime;
use futures::SinkExt;
use regex::Regex;
use serde_json::Value;
use serde_json::json;
use sonic_rs::to_object_iter_unchecked;
use sonic_rs::{Deserialize, JsonValueTrait, Serialize};
use std::collections::HashMap;

use fastwebsockets::{FragmentCollector, Frame, OpCode};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;

use futures::channel::mpsc;
use futures::stream::Stream;

use super::{
    super::{
        Exchange, Kline, MarketType, OpenInterest, StreamType, Ticker, TickerInfo, TickerStats,
        Timeframe, Trade,
        connect::{State, setup_tcp_connection, setup_tls_connection, setup_websocket_connection},
        de_string_to_f32, de_string_to_u64,
        depth::{LocalDepthCache, Order, TempLocalDepth},
    },
    Connection, Event, StreamError,
};

fn de_iso8601_to_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let dt = DateTime::parse_from_rfc3339(&s).map_err(serde::de::Error::custom)?;
    Ok(dt.timestamp_millis() as u64)
}

#[derive(Serialize, Deserialize, Debug)]
struct SonicDepthSnapshot {
    #[serde(rename = "bids")]
    pub bids: Vec<Order>,
    #[serde(rename = "asks")]
    pub asks: Vec<Order>,
}

#[derive(Serialize, Deserialize, Debug)]
struct SonicDepthDelta {
    #[serde(rename = "time", deserialize_with = "de_iso8601_to_u64")]
    pub time: u64,
    #[serde(rename = "changes")]
    pub changes: Vec<SonicDepthChange>,
}

#[derive(Serialize, Deserialize, Debug)]
struct SonicDepthChange {
    #[serde(rename = "side")]
    pub side: String,
    #[serde(rename = "price", deserialize_with = "de_string_to_f32")]
    pub price: f32,
    #[serde(rename = "size", deserialize_with = "de_string_to_f32")]
    pub qty: f32,
}

#[derive(Serialize, Deserialize, Debug)]
struct SonicTrade {
    #[serde(rename = "time", deserialize_with = "de_iso8601_to_u64")]
    pub time: u64,
    #[serde(rename = "price", deserialize_with = "de_string_to_f32")]
    pub price: f32,
    #[serde(rename = "size", deserialize_with = "de_string_to_f32")]
    pub qty: f32,
    #[serde(rename = "side")]
    pub is_sell: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SonicKline {
    #[serde(rename = "start")]
    pub time: u64,
    #[serde(rename = "open", deserialize_with = "de_string_to_f32")]
    pub open: f32,
    #[serde(rename = "high", deserialize_with = "de_string_to_f32")]
    pub high: f32,
    #[serde(rename = "low", deserialize_with = "de_string_to_f32")]
    pub low: f32,
    #[serde(rename = "close", deserialize_with = "de_string_to_f32")]
    pub close: f32,
    #[serde(rename = "volume", deserialize_with = "de_string_to_f32")]
    pub volume: f32,
    #[serde(rename = "interval")]
    pub interval: String,
}

#[derive(Debug)]
enum StreamData {
    Trade(Ticker, SonicTrade),
    DepthSnapshot(Ticker, SonicDepthSnapshot),
    DepthDelta(Ticker, SonicDepthDelta, u64),
}

#[derive(Debug)]
enum StreamName {
    DepthSnapshot,
    DepthDelta,
    Trade,
    Unknown,
}

impl StreamName {
    fn from_topic(topic: &str) -> Self {
        match topic {
            "match" => StreamName::Trade,
            "snapshot" => StreamName::DepthSnapshot,
            "l2update" => StreamName::DepthDelta,
            _ => StreamName::Unknown,
        }
    }
}

#[derive(Debug)]
enum StreamWrapper {
    Trade,
    DepthSnapshot,
    DepthDelta,
    Kline,
}

#[allow(unused_assignments)]
fn feed_de(slice: &[u8]) -> Result<StreamData, StreamError> {
    let mut stream_type: Option<StreamWrapper> = None;

    let mut topic_ticker: Option<Ticker> = None;

    let iter: sonic_rs::ObjectJsonIter = unsafe { to_object_iter_unchecked(slice) };

    for elem in iter {
        let (k, v) = elem.map_err(|e| StreamError::ParseError(e.to_string()))?;

        if k == "type" {
            if let Some(val) = v.as_str() {
                match StreamName::from_topic(val) {
                    StreamName::DepthSnapshot => {
                        stream_type = Some(StreamWrapper::DepthSnapshot);
                    }
                    StreamName::DepthDelta => {
                        stream_type = Some(StreamWrapper::DepthDelta);
                    }
                    StreamName::Trade => {
                        stream_type = Some(StreamWrapper::Trade);
                    }
                    _ => {
                        log::error!("Unknown stream name: {}", val);
                    }
                }
            }
        } else if k == "product_id" {
            if let Some(val) = v.as_str() {
                topic_ticker = Some(Ticker::new(val, MarketType::Spot));
            }
        }
    }

    match stream_type {
        Some(StreamWrapper::Trade) => {
            let trade: SonicTrade = sonic_rs::from_slice(slice).map_err(|e| {
                log::error!("Failed to parse trade data: {}", e);
                StreamError::ParseError(e.to_string())
            })?;

            return Ok(StreamData::Trade(
                topic_ticker.expect("couldnt get ticker topic"),
                trade,
            ));
        }
        Some(StreamWrapper::DepthSnapshot) => {
            let depth_snapshot: SonicDepthSnapshot = sonic_rs::from_slice(slice).map_err(|e| {
                log::error!("Failed to parse depth snapshot data: {}", e);
                StreamError::ParseError(e.to_string())
            })?;

            return Ok(StreamData::DepthSnapshot(
                topic_ticker.expect("couldnt get ticker topic"),
                depth_snapshot,
            ));
        }
        Some(StreamWrapper::DepthDelta) => {
            let depth_delta: SonicDepthDelta = sonic_rs::from_slice(slice).map_err(|e| {
                log::error!("Failed to parse depth delta data: {}", e);
                StreamError::ParseError(e.to_string())
            })?;

            let time = depth_delta.time;
            return Ok(StreamData::DepthDelta(
                topic_ticker.expect("couldnt get ticker topic"),
                depth_delta,
                time,
            ));
        }
        _ => log::error!("Unknown stream type"),
    }

    Err(StreamError::UnknownError("Unknown data".to_string()))
}

async fn connect(domain: &str) -> Result<FragmentCollector<TokioIo<Upgraded>>, StreamError> {
    let tcp_stream = setup_tcp_connection(domain).await?;
    let tls_stream = setup_tls_connection(domain, tcp_stream).await?;
    let url = format!("wss://{domain}");
    setup_websocket_connection(domain, tls_stream, &url).await
}

async fn try_connect(
    streams: &Value,
    market_type: MarketType,
    output: &mut mpsc::Sender<Event>,
) -> State {
    let exchange = match market_type {
        MarketType::Spot => Exchange::CoinbaseSpot,
        _ => {
            log::error!("Unsupported market type for Coinbase: {:?}", market_type);
            return State::Disconnected;
        }
    };

    match connect("ws-feed.exchange.coinbase.com").await {
        Ok(mut websocket) => {
            if let Err(e) = websocket
                .write_frame(Frame::text(fastwebsockets::Payload::Borrowed(
                    streams.to_string().as_bytes(),
                )))
                .await
            {
                let _ = output
                    .send(Event::Disconnected(
                        exchange,
                        format!("Failed subscribing: {e}"),
                    ))
                    .await;
                return State::Disconnected;
            }

            let _ = output.send(Event::Connected(exchange, Connection)).await;
            State::Connected(websocket)
        }
        Err(err) => {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

            let _ = output
                .send(Event::Disconnected(
                    exchange,
                    format!("Failed to connect: {err}"),
                ))
                .await;
            State::Disconnected
        }
    }
}

pub fn connect_market_stream(
    tickers: Vec<Ticker>,
    market_type: MarketType,
) -> impl Stream<Item = Event> {
    super::channel(100, async move |mut output| {
        let mut state: State = State::Disconnected;

        let product_ids: Vec<String> = tickers
            .iter()
            .map(|ticker| ticker.get_string().0.to_uppercase())
            .collect();

        let exchange = match market_type {
            MarketType::Spot => Exchange::CoinbaseSpot,
            _ => {
                log::error!("Unsupported market type for Coinbase: {:?}", market_type);
                return;
            }
        };

        let subscribe_message = serde_json::json!({
            "type": "subscribe",
            "product_ids": product_ids,
            "channels": [
                "matches",
                "level2_batch"
            ]
        });

        let mut trades_buffers: HashMap<Ticker, Vec<Trade>> = HashMap::new();
        let mut orderbooks: HashMap<Ticker, LocalDepthCache> = HashMap::new();

        for ticker in &tickers {
            trades_buffers.insert(*ticker, Vec::new());
            orderbooks.insert(*ticker, LocalDepthCache::new());
        }

        loop {
            match &mut state {
                State::Disconnected => {
                    state = try_connect(&subscribe_message, market_type, &mut output).await;
                }
                State::Connected(websocket) => match websocket.read_frame().await {
                    Ok(msg) => match msg.opcode {
                        OpCode::Text => {
                            match feed_de(&msg.payload[..]) {
                                Ok(data) => match data {
                                    StreamData::Trade(ticker, de_trade) => {
                                        if let Some(buffer) = trades_buffers.get_mut(&ticker) {
                                            let trade = Trade {
                                                time: de_trade.time,
                                                is_sell: de_trade.is_sell == "Buy",
                                                price: de_trade.price,
                                                qty: de_trade.qty,
                                            };

                                            buffer.push(trade);
                                        }
                                    }
                                    StreamData::DepthDelta(ticker, de_depth, time) => {
                                        if let Some(orderbook) = orderbooks.get_mut(&ticker) {
                                            let bids = de_depth
                                                .changes
                                                .iter()
                                                .filter(|x| x.side == "buy")
                                                .map(|x| Order {
                                                    price: x.price,
                                                    qty: x.qty,
                                                })
                                                .collect::<Vec<Order>>();

                                            let asks = de_depth
                                                .changes
                                                .iter()
                                                .filter(|x| x.side == "sell")
                                                .map(|x| Order {
                                                    price: x.price,
                                                    qty: x.qty,
                                                })
                                                .collect::<Vec<Order>>();

                                            let depth_update = TempLocalDepth {
                                                last_update_id: 0,
                                                time,
                                                bids,
                                                asks,
                                            };

                                            orderbook.update_depth_cache(&depth_update);

                                            // Get trades for this ticker
                                            if let Some(trades) = trades_buffers.get_mut(&ticker) {
                                                let _ = output
                                                    .send(Event::DepthReceived(
                                                        StreamType::DepthAndTrades {
                                                            exchange,
                                                            ticker,
                                                        },
                                                        time,
                                                        orderbook.get_depth(),
                                                        std::mem::take(trades).into_boxed_slice(),
                                                    ))
                                                    .await;
                                            }
                                        }
                                    }
                                    StreamData::DepthSnapshot(ticker, de_depth) => {
                                        if let Some(orderbook) = orderbooks.get_mut(&ticker) {
                                            let temp_depth = TempLocalDepth {
                                                last_update_id: 0,
                                                time: 0,
                                                bids: de_depth
                                                    .bids
                                                    .iter()
                                                    .map(|x| Order {
                                                        price: x.price,
                                                        qty: x.qty,
                                                    })
                                                    .collect(),
                                                asks: de_depth
                                                    .asks
                                                    .iter()
                                                    .map(|x| Order {
                                                        price: x.price,
                                                        qty: x.qty,
                                                    })
                                                    .collect(),
                                            };

                                            orderbook.fetched(&temp_depth);
                                        }
                                    }
                                },
                                Err(e) => {
                                    log::error!("Failed to parse data: {}", e);
                                }
                            }
                        }
                        OpCode::Close => {
                            state = State::Disconnected;
                            let _ = output
                                .send(Event::Disconnected(
                                    exchange,
                                    "Connection closed".to_string(),
                                ))
                                .await;
                        }
                        _ => {}
                    },
                    Err(e) => {
                        state = State::Disconnected;
                        let _ = output
                            .send(Event::Disconnected(
                                exchange,
                                "Error reading frame: ".to_string() + &e.to_string(),
                            ))
                            .await;
                    }
                },
            }
        }
    })
}

pub fn connect_kline_stream(
    streams: Vec<(Ticker, Timeframe)>,
    market_type: MarketType,
) -> impl Stream<Item = Event> {
    super::channel(100, async move |mut output| {
        let mut state = State::Disconnected;

        let exchange = match market_type {
            MarketType::Spot => Exchange::CoinbaseSpot,
            _ => {
                log::error!("Unsupported market type for Coinbase: {:?}", market_type);
                return;
            }
        };

        let stream_str = streams
            .iter()
            .map(|(ticker, timeframe)| {
                let timeframe_str = timeframe.to_minutes().to_string();
                format!("kline.{timeframe_str}.{}", ticker.get_string().0)
            })
            .collect::<Vec<String>>();

        let subscribe_message = serde_json::json!({
            "op": "subscribe",
            "args": stream_str
        });

        loop {
            match &mut state {
                State::Disconnected => {
                    state = try_connect(&subscribe_message, market_type, &mut output).await;
                }
                State::Connected(websocket) => match websocket.read_frame().await {
                    Ok(msg) => match msg.opcode {
                        OpCode::Text => unimplemented!(),
                        OpCode::Close => {
                            state = State::Disconnected;
                            let _ = output
                                .send(Event::Disconnected(
                                    exchange,
                                    "Connection closed".to_string(),
                                ))
                                .await;
                        }
                        _ => {}
                    },
                    Err(e) => {
                        state = State::Disconnected;
                        let _ = output
                            .send(Event::Disconnected(
                                exchange,
                                "Error reading frame: ".to_string() + &e.to_string(),
                            ))
                            .await;
                    }
                },
            }
        }
    })
}

fn string_to_timeframe(interval: &str) -> Option<Timeframe> {
    Timeframe::ALL
        .iter()
        .find(|&tf| tf.to_minutes().to_string() == interval)
        .copied()
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeOpenInterest {
    #[serde(rename = "openInterest", deserialize_with = "de_string_to_f32")]
    pub value: f32,
    #[serde(deserialize_with = "de_string_to_u64")]
    pub timestamp: u64,
}

pub async fn fetch_historical_oi(
    ticker: Ticker,
    range: Option<(u64, u64)>,
    period: Timeframe,
) -> Result<Vec<OpenInterest>, StreamError> {
    let ticker_str = ticker.get_string().0.to_uppercase();
    let period_str = match period {
        Timeframe::M5 => "5min",
        Timeframe::M15 => "15min",
        Timeframe::M30 => "30min",
        Timeframe::H1 => "1h",
        Timeframe::H2 => "2h",
        Timeframe::H4 => "4h",
        _ => {
            let err_msg = format!("Unsupported timeframe for open interest: {period}");
            log::error!("{}", err_msg);
            return Err(StreamError::UnknownError(err_msg));
        }
    };

    let mut url = format!(
        "https://api.bybit.com/v5/market/open-interest?category=linear&symbol={ticker_str}&intervalTime={period_str}",
    );

    if let Some((start, end)) = range {
        let interval_ms = period.to_milliseconds();
        let num_intervals = ((end - start) / interval_ms).min(200);

        url.push_str(&format!(
            "&startTime={start}&endTime={end}&limit={num_intervals}"
        ));
    } else {
        url.push_str("&limit=200");
    }

    let response = reqwest::get(&url).await.map_err(|e| {
        log::error!("Failed to fetch from {}: {}", url, e);
        StreamError::FetchError(e)
    })?;

    let text = response.text().await.map_err(|e| {
        log::error!("Failed to get response text from {}: {}", url, e);
        StreamError::FetchError(e)
    })?;

    let content: Value = sonic_rs::from_str(&text).map_err(|e| {
        log::error!(
            "Failed to parse JSON from {}: {}\nResponse: {}",
            url,
            e,
            text
        );
        StreamError::ParseError(e.to_string())
    })?;

    let result_list = content["result"]["list"].as_array().ok_or_else(|| {
        log::error!("Result list is not an array in response: {}", text);
        StreamError::ParseError("Result list is not an array".to_string())
    })?;

    let bybit_oi: Vec<DeOpenInterest> =
        serde_json::from_value(json!(result_list)).map_err(|e| {
            log::error!(
                "Failed to parse open interest array: {}\nResponse: {}",
                e,
                text
            );
            StreamError::ParseError(format!("Failed to parse open interest: {e}"))
        })?;

    let open_interest: Vec<OpenInterest> = bybit_oi
        .into_iter()
        .map(|x| OpenInterest {
            time: x.timestamp,
            value: x.value,
        })
        .collect();

    if open_interest.is_empty() {
        log::warn!(
            "No open interest data found for {}, from url: {}",
            ticker_str,
            url
        );
    }

    Ok(open_interest)
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct ApiResponse {
    #[serde(rename = "retCode")]
    ret_code: u32,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: ApiResult,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct ApiResult {
    symbol: String,
    category: String,
    list: Vec<Vec<Value>>,
}

pub async fn fetch_klines(
    ticker: Ticker,
    timeframe: Timeframe,
    range: Option<(u64, u64)>,
) -> Result<Vec<Kline>, StreamError> {
    let (symbol_str, market_type) = &ticker.get_string();
    let timeframe_str = timeframe.to_minutes().to_string();

    fn parse_kline_field<T: std::str::FromStr>(field: Option<&str>) -> Result<T, StreamError> {
        field
            .ok_or_else(|| StreamError::ParseError("Failed to parse kline".to_string()))
            .and_then(|s| {
                s.parse::<T>()
                    .map_err(|_| StreamError::ParseError("Failed to parse kline".to_string()))
            })
    }

    let market = match market_type {
        MarketType::Spot => "spot",
        MarketType::LinearPerps => "linear",
        MarketType::InversePerps => "inverse",
    };

    let mut url = format!(
        "https://api.bybit.com/v5/market/kline?category={}&symbol={}&interval={}",
        market,
        symbol_str.to_uppercase(),
        timeframe_str
    );

    if let Some((start, end)) = range {
        let interval_ms = timeframe.to_milliseconds();
        let num_intervals = ((end - start) / interval_ms).min(1000);

        url.push_str(&format!("&start={start}&end={end}&limit={num_intervals}"));
    } else {
        url.push_str(&format!("&limit={}", 200));
    }

    let response: reqwest::Response = reqwest::get(&url).await.map_err(StreamError::FetchError)?;
    let text = response.text().await.map_err(StreamError::FetchError)?;

    let api_response: ApiResponse =
        sonic_rs::from_str(&text).map_err(|e| StreamError::ParseError(e.to_string()))?;

    let klines: Result<Vec<Kline>, StreamError> = api_response
        .result
        .list
        .iter()
        .map(|kline| {
            let time = parse_kline_field::<u64>(kline[0].as_str())?;
            let open = parse_kline_field::<f32>(kline[1].as_str())?;
            let high = parse_kline_field::<f32>(kline[2].as_str())?;
            let low = parse_kline_field::<f32>(kline[3].as_str())?;
            let close = parse_kline_field::<f32>(kline[4].as_str())?;
            let volume = parse_kline_field::<f32>(kline[5].as_str())?;

            Ok(Kline {
                time,
                open,
                high,
                low,
                close,
                volume: (-1.0, volume),
            })
        })
        .collect();

    klines
}

pub async fn fetch_ticksize(
    market_type: MarketType,
) -> Result<HashMap<Ticker, Option<TickerInfo>>, StreamError> {
    let market = match market_type {
        MarketType::Spot => "spot",
        MarketType::LinearPerps => "linear",
        MarketType::InversePerps => "inverse",
    };

    let url =
        format!("https://api.bybit.com/v5/market/instruments-info?category={market}&limit=1000",);

    let response = reqwest::get(&url).await.map_err(StreamError::FetchError)?;
    let text = response.text().await.map_err(StreamError::FetchError)?;

    let exchange_info: Value =
        sonic_rs::from_str(&text).map_err(|e| StreamError::ParseError(e.to_string()))?;

    let result_list: &Vec<Value> = exchange_info["result"]["list"]
        .as_array()
        .ok_or_else(|| StreamError::ParseError("Result list is not an array".to_string()))?;

    let mut ticker_info_map = HashMap::new();

    let re = Regex::new(r"^[a-zA-Z0-9]+$").unwrap();

    for item in result_list {
        let symbol = item["symbol"]
            .as_str()
            .ok_or_else(|| StreamError::ParseError("Symbol not found".to_string()))?;

        if !re.is_match(symbol) {
            continue;
        }

        let price_filter = item["priceFilter"]
            .as_object()
            .ok_or_else(|| StreamError::ParseError("Price filter not found".to_string()))?;

        let min_ticksize = price_filter["tickSize"]
            .as_str()
            .ok_or_else(|| StreamError::ParseError("Tick size not found".to_string()))?
            .parse::<f32>()
            .map_err(|_| StreamError::ParseError("Failed to parse tick size".to_string()))?;

        let ticker = Ticker::new(symbol, market_type);

        ticker_info_map.insert(
            ticker,
            Some(TickerInfo {
                ticker,
                min_ticksize,
            }),
        );
    }

    Ok(ticker_info_map)
}

const PERP_FILTER_VOLUME: f32 = 12_000_000.0;
const SPOT_FILTER_VOLUME: f32 = 4_000_000.0;

pub async fn fetch_ticker_prices(
    market_type: MarketType,
) -> Result<HashMap<Ticker, TickerStats>, StreamError> {
    let (market, volume_threshold) = match market_type {
        MarketType::Spot => ("spot", SPOT_FILTER_VOLUME),
        MarketType::LinearPerps => ("linear", PERP_FILTER_VOLUME),
        MarketType::InversePerps => ("inverse", PERP_FILTER_VOLUME),
    };

    let url = format!("https://api.bybit.com/v5/market/tickers?category={market}");
    let response = reqwest::get(&url).await.map_err(StreamError::FetchError)?;
    let text = response.text().await.map_err(StreamError::FetchError)?;

    let exchange_info: Value =
        sonic_rs::from_str(&text).map_err(|e| StreamError::ParseError(e.to_string()))?;

    let result_list: &Vec<Value> = exchange_info["result"]["list"]
        .as_array()
        .ok_or_else(|| StreamError::ParseError("Result list is not an array".to_string()))?;

    let mut ticker_prices_map = HashMap::new();

    let re = Regex::new(r"^[a-zA-Z0-9]+$").unwrap();

    for item in result_list {
        let symbol = item["symbol"]
            .as_str()
            .ok_or_else(|| StreamError::ParseError("Symbol not found".to_string()))?;

        if !re.is_match(symbol) {
            continue;
        }

        let mark_price = item["lastPrice"]
            .as_str()
            .ok_or_else(|| StreamError::ParseError("Mark price not found".to_string()))?
            .parse::<f32>()
            .map_err(|_| StreamError::ParseError("Failed to parse mark price".to_string()))?;

        let daily_price_chg = item["price24hPcnt"]
            .as_str()
            .ok_or_else(|| StreamError::ParseError("Daily price change not found".to_string()))?
            .parse::<f32>()
            .map_err(|_| {
                StreamError::ParseError("Failed to parse daily price change".to_string())
            })?;

        let daily_volume = item["volume24h"]
            .as_str()
            .ok_or_else(|| StreamError::ParseError("Daily volume not found".to_string()))?
            .parse::<f32>()
            .map_err(|_| StreamError::ParseError("Failed to parse daily volume".to_string()))?;

        let quote_volume = daily_volume * mark_price;

        if quote_volume < volume_threshold {
            continue;
        }

        let ticker_stats = TickerStats {
            mark_price,
            daily_price_chg: daily_price_chg * 100.0,
            daily_volume: quote_volume,
        };

        ticker_prices_map.insert(Ticker::new(symbol, market_type), ticker_stats);
    }

    Ok(ticker_prices_map)
}
