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

#[derive(Serialize, Deserialize, Debug)]
struct SonicDepth {
    #[serde(rename = "u")]
    pub update_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<Order>,
    #[serde(rename = "a")]
    pub asks: Vec<Order>,
}

#[derive(Serialize, Deserialize, Debug)]
struct SonicTrade {
    #[serde(rename = "T")]
    pub time: u64,
    #[serde(rename = "p", deserialize_with = "de_string_to_f32")]
    pub price: f32,
    #[serde(rename = "v", deserialize_with = "de_string_to_f32")]
    pub qty: f32,
    #[serde(rename = "S")]
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
    Trade(Ticker, Vec<SonicTrade>),
    Depth(Ticker, SonicDepth, String, u64),
    Kline(Ticker, Vec<SonicKline>),
}

#[derive(Debug)]
enum StreamName {
    Depth(Ticker),
    Trade(Ticker),
    Kline(Ticker),
    Unknown,
}

impl StreamName {
    fn from_topic(topic: &str, market_type: MarketType) -> Self {
        let parts: Vec<&str> = topic.split('.').collect();

        if parts.is_empty() {
            return StreamName::Unknown;
        }

        match parts[0] {
            "publicTrade" => {
                if parts.len() >= 2 {
                    let ticker = Ticker::new(parts[1], market_type);
                    StreamName::Trade(ticker)
                } else {
                    StreamName::Unknown
                }
            }
            "orderbook" => {
                if parts.len() >= 3 {
                    //println!("{:?}, {}", market_type, parts[2]);

                    let ticker = Ticker::new(parts[2], market_type);
                    StreamName::Depth(ticker)
                } else {
                    StreamName::Unknown
                }
            }
            "kline" => {
                if parts.len() >= 3 {
                    let ticker = Ticker::new(parts[2], market_type);
                    StreamName::Kline(ticker)
                } else {
                    StreamName::Unknown
                }
            }
            _ => StreamName::Unknown,
        }
    }
}

#[derive(Debug)]
enum StreamWrapper {
    Trade,
    Depth,
    Kline,
}

#[allow(unused_assignments)]
fn feed_de(slice: &[u8], market_type: MarketType) -> Result<StreamData, StreamError> {
    let mut stream_type: Option<StreamWrapper> = None;
    let mut depth_wrap: Option<SonicDepth> = None;

    let mut data_type = String::new();
    let mut topic_ticker: Option<Ticker> = None;

    let iter: sonic_rs::ObjectJsonIter = unsafe { to_object_iter_unchecked(slice) };

    for elem in iter {
        let (k, v) = elem.map_err(|e| StreamError::ParseError(e.to_string()))?;

        if k == "topic" {
            if let Some(val) = v.as_str() {
                match StreamName::from_topic(val, market_type) {
                    StreamName::Depth(t) => {
                        stream_type = Some(StreamWrapper::Depth);
                        topic_ticker = Some(t);
                    }
                    StreamName::Trade(t) => {
                        stream_type = Some(StreamWrapper::Trade);
                        topic_ticker = Some(t);
                    }
                    StreamName::Kline(t) => {
                        stream_type = Some(StreamWrapper::Kline);
                        topic_ticker = Some(t);
                    }
                    _ => {
                        log::error!("Unknown stream name");
                    }
                }
            }
        } else if k == "type" {
            v.as_str().unwrap().clone_into(&mut data_type);
        } else if k == "data" {
            match stream_type {
                Some(StreamWrapper::Trade) => {
                    let trade_wrap: Vec<SonicTrade> = sonic_rs::from_str(&v.as_raw_faststr())
                        .map_err(|e| StreamError::ParseError(e.to_string()))?;

                    return Ok(StreamData::Trade(
                        topic_ticker.expect("couldnt get ticker topic"),
                        trade_wrap,
                    ));
                }
                Some(StreamWrapper::Depth) => {
                    if depth_wrap.is_none() {
                        depth_wrap = Some(SonicDepth {
                            update_id: 0,
                            bids: Vec::new(),
                            asks: Vec::new(),
                        });
                    }
                    depth_wrap = Some(
                        sonic_rs::from_str(&v.as_raw_faststr())
                            .map_err(|e| StreamError::ParseError(e.to_string()))?,
                    );
                }
                Some(StreamWrapper::Kline) => {
                    let kline_wrap: Vec<SonicKline> = sonic_rs::from_str(&v.as_raw_faststr())
                        .map_err(|e| StreamError::ParseError(e.to_string()))?;

                    if let Some(t) = topic_ticker {
                        return Ok(StreamData::Kline(t, kline_wrap));
                    } else {
                        return Err(StreamError::ParseError(
                            "Missing ticker for kline data".to_string(),
                        ));
                    }
                }
                _ => {
                    log::error!("Unknown stream type");
                }
            }
        } else if k == "cts" {
            if let Some(dw) = depth_wrap {
                let time: u64 = v
                    .as_u64()
                    .ok_or_else(|| StreamError::ParseError("Failed to parse u64".to_string()))?;

                return Ok(StreamData::Depth(
                    topic_ticker.expect("couldnt get ticker topic"),
                    dw,
                    data_type.to_string(),
                    time,
                ));
            }
        }
    }

    Err(StreamError::UnknownError("Unknown data".to_string()))
}

async fn connect(
    domain: &str,
    market_type: MarketType,
) -> Result<FragmentCollector<TokioIo<Upgraded>>, StreamError> {
    let tcp_stream = setup_tcp_connection(domain).await?;
    let tls_stream = setup_tls_connection(domain, tcp_stream).await?;
    let url = format!(
        "wss://stream.bybit.com/v5/public/{}",
        match market_type {
            MarketType::Spot => "spot",
            MarketType::LinearPerps => "linear",
            MarketType::InversePerps => "inverse",
        }
    );
    setup_websocket_connection(domain, tls_stream, &url).await
}

async fn try_connect(
    streams: &Value,
    market_type: MarketType,
    output: &mut mpsc::Sender<Event>,
) -> State {
    let exchange = match market_type {
        MarketType::Spot => Exchange::BybitSpot,
        MarketType::LinearPerps => Exchange::BybitLinear,
        MarketType::InversePerps => Exchange::BybitInverse,
    };

    match connect("stream.bybit.com", market_type).await {
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

        let exchange = match market_type {
            MarketType::Spot => Exchange::BybitSpot,
            MarketType::LinearPerps => Exchange::BybitLinear,
            MarketType::InversePerps => Exchange::BybitInverse,
        };

        let trade_stream_args: Vec<String> = tickers
            .iter()
            .map(|t| format!("publicTrade.{}", t.get_string().0))
            .collect::<Vec<String>>();

        let book_stream_args: Vec<String> = tickers
            .iter()
            .map(|t| {
                format!(
                    "orderbook.{}.{}",
                    match market_type {
                        MarketType::Spot => "200",
                        MarketType::LinearPerps | MarketType::InversePerps => "500",
                    },
                    t.get_string().0
                )
            })
            .collect::<Vec<String>>();

        let mut stream_args = Vec::new();
        stream_args.extend(trade_stream_args);
        stream_args.extend(book_stream_args);

        let subscribe_message = serde_json::json!({
            "op": "subscribe",
            "args": stream_args
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
                            match feed_de(&msg.payload[..], market_type) {
                                Ok(data) => match data {
                                    StreamData::Trade(ticker, de_trade_vec) => {
                                        if let Some(buffer) = trades_buffers.get_mut(&ticker) {
                                            for de_trade in &de_trade_vec {
                                                let trade = Trade {
                                                    time: de_trade.time,
                                                    is_sell: de_trade.is_sell == "Sell",
                                                    price: de_trade.price,
                                                    qty: de_trade.qty,
                                                };

                                                buffer.push(trade);
                                            }
                                        }
                                    }
                                    StreamData::Depth(ticker, de_depth, data_type, time) => {
                                        if let Some(orderbook) = orderbooks.get_mut(&ticker) {
                                            let depth_update = TempLocalDepth {
                                                last_update_id: de_depth.update_id,
                                                time,
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

                                            if (data_type == "snapshot")
                                                || (depth_update.last_update_id == 1)
                                            {
                                                orderbook.fetched(&depth_update);
                                            } else if data_type == "delta" {
                                                orderbook.update_depth_cache(&depth_update);

                                                // Get trades for this ticker
                                                if let Some(trades) =
                                                    trades_buffers.get_mut(&ticker)
                                                {
                                                    let _ = output
                                                        .send(Event::DepthReceived(
                                                            StreamType::DepthAndTrades {
                                                                exchange,
                                                                ticker,
                                                            },
                                                            time,
                                                            orderbook.get_depth(),
                                                            std::mem::take(trades)
                                                                .into_boxed_slice(),
                                                        ))
                                                        .await;
                                                }
                                            }
                                        }
                                    }
                                    _ => {
                                        log::warn!("Unknown data: {:?}", &data);
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
            MarketType::Spot => Exchange::BybitSpot,
            MarketType::LinearPerps => Exchange::BybitLinear,
            MarketType::InversePerps => Exchange::BybitInverse,
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
                        OpCode::Text => {
                            if let Ok(StreamData::Kline(ticker, de_kline_vec)) =
                                feed_de(&msg.payload[..], market_type)
                            {
                                for de_kline in &de_kline_vec {
                                    let kline = Kline {
                                        time: de_kline.time,
                                        open: de_kline.open,
                                        high: de_kline.high,
                                        low: de_kline.low,
                                        close: de_kline.close,
                                        volume: (-1.0, de_kline.volume),
                                    };

                                    if let Some(timeframe) = string_to_timeframe(&de_kline.interval)
                                    {
                                        let _ = output
                                            .send(Event::KlineReceived(
                                                StreamType::Kline {
                                                    exchange,
                                                    ticker,
                                                    timeframe,
                                                },
                                                kline,
                                            ))
                                            .await;
                                    } else {
                                        log::error!(
                                            "Failed to find timeframe: {}, {:?}",
                                            &de_kline.interval,
                                            streams
                                        );
                                    }
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
