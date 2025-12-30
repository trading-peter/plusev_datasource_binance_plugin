# Binance datasource plugin (PlusEV)

This is a PlusEV `datasrc` (datasource) plugin for Binance.

It is implemented to mirror the structure of `plusev_datasource_woox_plugin`, but uses the Binance REST + WebSocket message formats.

## Supported commands

- `getMarkets`
- `getTimeframes`
- `getOHLCV`
- `ohlcvStream`

## Configuration

- `apiKey` (optional)
- `apiSecret` (optional)

These are only required for private endpoints (not implemented yet). Public market data works without credentials.

## Build

```bash
./build.sh
```

This produces `plugin.wasm`.

## Notes / TODO

- Currently uses Binance spot endpoints (`https://api.binance.com`) and spot WS (`wss://stream.binance.com:9443/ws`).
- Futures support can be added by switching to `fapi.binance.com` + `fstream.binance.com` and mapping the futures exchange-info + klines as needed.
