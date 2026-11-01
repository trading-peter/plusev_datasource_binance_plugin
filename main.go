package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	ex "github.com/plusev-terminal/go-plugin-common/v2/datasrc/exchange"
	acct "github.com/plusev-terminal/go-plugin-common/v2/datasrc/exchange/account"
	m "github.com/plusev-terminal/go-plugin-common/v2/meta"
	"github.com/plusev-terminal/go-plugin-common/v2/plugin"
	"github.com/plusev-terminal/go-plugin-common/v2/requester"
	commonstream "github.com/plusev-terminal/go-plugin-common/v2/stream"
	tt "github.com/plusev-terminal/go-plugin-common/v2/trading"

	"github.com/trading-peter/plusev_datasource_binance_plugin/binance"
)

// ============================================================================
// Main - Register the plugin
// ============================================================================

func init() {
	// Create plugin instance
	p := &BinancePlugin{}

	// Register the plugin - this generates all standard WASM exports automatically
	// IMPORTANT: Must be in init(), not main(), so it runs before WASM exports are called
	plugin.RegisterPlugin(p)

	// Register stream handler - this generates handle_stream_message and handle_connection_event exports
	// The client implements the StreamHandler interface (HandleStreamMessage, HandleConnectionEvent)
	// NOTE: client will be initialized in OnInit, but we register it here so the exports are available
	// The actual client instance will be set when OnInit is called
	plugin.RegisterStreamHandler(&streamHandlerWrapper{p: p})
}

func main() {
	// Required for WASM, but can be empty
}

// ============================================================================
// Plugin Implementation
// ============================================================================

// BinancePlugin implements the DataSourcePlugin interface
type BinancePlugin struct {
	config *plugin.ConfigStore
	client *binance.Client
}

// GetMeta returns the plugin metadata
func (p *BinancePlugin) GetMeta() m.Meta {
	return m.Meta{
		PluginID:    "binance-datasource",
		Name:        "Binance Exchange",
		AppID:       "datasrc",
		Category:    "cex",
		Description: "Adds support for Binance Exchange",
		Author:      "trading_peter",
		Version: "v1.2.0",
		Repository:  "https://github.com/trading-peter/plusev_datasource_binance_plugin",
		Tags:        []string{"binance", "crypto", "exchange", "spot", "futures"},
		Contacts: []m.AuthorContact{
			{
				Kind:  "x.com",
				Value: "https://x.com/trading_peter",
			},
		},
		Resources: m.ResourceAccess{
			AllowedNetworkTargets: []m.NetworkTargetRule{
				{
					Pattern:        "https://api.binance.com/*",
					Limits:         spotRateLimits(),
					TradingReserve: &tradingReserve,
					// Server usage headers for the Rate Limits page display.
					// Declared here since Terminal no longer hardcodes
					// venue-specific header names.
					UsageHeaders: binanceUsageHeaders(),
				},
				{
					Pattern:        "https://fapi.binance.com/*",
					Limits:         futuresRateLimits(),
					TradingReserve: &tradingReserve,
					UsageHeaders:   binanceUsageHeaders(),
				},
				// Spot klines streaming (PrepareStream). Was missing, so spot WS
				// connections were refused by the network policy (only fstream
				// was allow-listed).
				{Pattern: "wss://stream.binance.com:9443/*"},
				{Pattern: "wss://fstream.binance.com/*"},
			},
			FsWriteAccess: map[string]string{
				"logs": "/logs",
			},
		},
		// Features will be auto-populated with registered commands
		Features: []string{},
	}
}

// GetRateLimits intentionally returns an empty array: Binance's real budgets
// are declared as manifest limits on the network targets in GetMeta (see the
// declarations below), and opting out of the legacy per-command limiter keeps
// the two layers from double-throttling the same requests.
func (p *BinancePlugin) GetRateLimits() []plugin.RateLimit {
	return []plugin.RateLimit{}
}

// ============================================================================
// Rate-limit declarations (manifest limits enforced by the host)
// ============================================================================
//
// Source URLs for every number in this section and for the per-request Cost
// weights in binance/client.go (all retrieved 2026-10-31):
//
//   - Spot limits (REQUEST_WEIGHT / ORDERS / RAW_REQUESTS):
//     https://developers.binance.com/en/docs/products/spot/rest-api (LIMITS)
//     and live https://api.binance.com/api/v3/exchangeInfo rateLimits[]
//   - USDⓈ-M futures limits (REQUEST_WEIGHT / ORDERS):
//     https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info (LIMITS)
//     and live https://fapi.binance.com/fapi/v1/exchangeInfo rateLimits[]
//   - Spot per-endpoint weights:
//     https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/general (exchangeInfo = 20)
//     https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market (klines = 2)
//     https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/account (account = 20, openOrders = 6/80, allOrders = 20)
//   - Futures per-endpoint weights:
//     https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data (exchangeInfo = 1, klines = 1/2/5/10 by limit)
//     https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account (account = 5, balance = 5)
//     https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade (openOrders = 1/40, allOrders = 5, positionRisk = 5)
//
// Burst is left at the default (= limit): Binance publishes no separate burst
// and its windows are rolling, so one full window of units is the faithful
// bucket capacity.

// tradingReserve reserves 30% of every budget on the REST hosts for order
// commands (createOrders/cancelOrders/modifyOrders), which share these hosts
// with data pulls.
var tradingReserve = 0.3

// spotRateLimits declares the published budgets of https://api.binance.com.
func spotRateLimits() []m.RateLimitRule {
	return []m.RateLimitRule{
		// REQUEST_WEIGHT: 6000 weight/min per IP.
		// https://developers.binance.com/en/docs/products/spot/rest-api (confirmed live in https://api.binance.com/api/v3/exchangeInfo)
		m.NewRateLimitRule("weight", 6000, time.Minute).WithLabel("Request weight").Weighted(),
		// RAW_REQUESTS: 300000 requests/5min per IP.
		// https://api.binance.com/api/v3/exchangeInfo (rateLimits array, retrieved 2026-10-31)
		m.NewRateLimitRule("raw", 300000, 5*time.Minute).WithLabel("Raw requests"),
		// ORDERS: 100 orders/10s per account, on order placement endpoints.
		// https://developers.binance.com/en/docs/products/spot/rest-api (confirmed live in https://api.binance.com/api/v3/exchangeInfo)
		m.NewRateLimitRule("orders-10s", 100, 10*time.Second).WithLabel("Order rate (10s)").PerCredential().ForPaths("/api/v3/order"),
		// ORDERS: 200000 orders/24h per account, on order placement endpoints.
		// https://api.binance.com/api/v3/exchangeInfo (rateLimits array, retrieved 2026-10-31)
		m.NewRateLimitRule("orders-24h", 200000, 24*time.Hour).WithLabel("Order rate (24h)").PerCredential().ForPaths("/api/v3/order"),
	}
}

// futuresRateLimits declares the published budgets of https://fapi.binance.com.
func futuresRateLimits() []m.RateLimitRule {
	return []m.RateLimitRule{
		// REQUEST_WEIGHT: 2400 weight/min per IP.
		// https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info (confirmed live in https://fapi.binance.com/fapi/v1/exchangeInfo)
		m.NewRateLimitRule("weight", 2400, time.Minute).WithLabel("Request weight").Weighted(),
		// ORDERS: 300 orders/10s per account, on order placement endpoints.
		// https://fapi.binance.com/fapi/v1/exchangeInfo (rateLimits array, retrieved 2026-10-31)
		m.NewRateLimitRule("orders-10s", 300, 10*time.Second).WithLabel("Order rate (10s)").PerCredential().ForPaths("/fapi/v1/order", "/fapi/v1/batchOrders"),
		// ORDERS: 1200 orders/1min per account, on order placement endpoints.
		// https://fapi.binance.com/fapi/v1/exchangeInfo (rateLimits array, retrieved 2026-10-31)
		m.NewRateLimitRule("orders-1m", 1200, time.Minute).WithLabel("Order rate (1m)").PerCredential().ForPaths("/fapi/v1/order", "/fapi/v1/batchOrders"),
	}
}

// binanceUsageHeaders declares the rate-limit usage response headers both
// REST hosts send (spot and USDⓈ-M futures), so the host can show live
// usage per window on its Rate Limits page. Display-only.
func binanceUsageHeaders() []m.UsageHeader {
	return []m.UsageHeader{
		{Prefix: "x-mbx-used-weight-", Name: "used-weight-", Field: m.UsageFieldUsed},
		{Prefix: "x-mbx-order-count-", Name: "order-count-", Field: m.UsageFieldUsed},
	}
}

// GetConfigFields returns the configuration fields needed by this plugin
func (p *BinancePlugin) GetConfigFields() []plugin.ConfigField {
	if p.client == nil {
		p.client = binance.NewClient(requester.NewRequester(), "https://api.binance.com")
	}
	return p.client.GetConfigFields()
}

// OnInit initializes the plugin with the configuration
func (p *BinancePlugin) OnInit(config *plugin.ConfigStore) error {
	p.config = config

	// Initialize client
	// We use the default requester which uses host functions
	req := requester.NewRequester()
	p.client = binance.NewClient(req, "https://api.binance.com")

	// Configure client with credentials if available
	credentials := map[string]string{}
	if apiKey := config.GetString("apiKey"); apiKey != "" {
		credentials["apiKey"] = apiKey
	}
	if apiSecret := config.GetString("apiSecret"); apiSecret != "" {
		credentials["apiSecret"] = apiSecret
	}
	if len(credentials) > 0 {
		p.client.SetCredentials(credentials)
	}

	return nil
}

// OnConfigChange handles configuration changes
// OnShutdown is called when the plugin is being shut down
func (p *BinancePlugin) OnShutdown() error {
	return nil
}

// RegisterCommands registers all command handlers
func (p *BinancePlugin) RegisterCommands(router *plugin.CommandRouter) {
	router.Register(ex.CMD_GET_MARKETS, p.handleGetMarkets)
	router.Register(ex.CMD_GET_TIMEFRAMES, p.handleGetTimeframes)
	router.Register(ex.CMD_OHLCV_STREAM, p.handleOHLCVStream)
	router.Register(ex.CMD_GET_OHLCV, p.handleGetOHLCV)
	router.Register(ex.CMD_GET_ACCOUNT, p.handleGetAccount)
	router.Register(ex.CMD_GET_BALANCES, p.handleGetBalances)
	router.Register(ex.CMD_GET_POSITIONS, p.handleGetPositions)
	router.Register(ex.CMD_GET_ORDERS, p.handleGetOrders)
}

// ============================================================================
// Command Handlers
// ============================================================================

func (p *BinancePlugin) handleGetMarkets(params map[string]any) plugin.Response {
	markets, err := p.client.GetMarkets()
	if err != nil {
		return plugin.ErrorResponse(err)
	}
	return plugin.SuccessResponse(markets, time.Hour*12)
}

func (p *BinancePlugin) handleGetTimeframes(params map[string]any) plugin.Response {
	return plugin.SuccessResponse(p.client.GetTimeframes(), time.Hour*12)
}

func (p *BinancePlugin) handleGetOHLCV(params map[string]any) plugin.Response {
	ohlcvParams := ex.GetOHLCVParamsFromMap(params)
	assetType := tt.AssetType(strings.TrimSpace(string(ohlcvParams.Market.AssetType)))
	if assetType == "" {
		assetType = tt.AssetTypeSpot
	}

	if strings.TrimSpace(ohlcvParams.Market.Symbol) == "" {
		return plugin.ErrorResponseMsg("market.symbol is required")
	}
	records, err := p.client.GetOHLCVWithAssetType(ohlcvParams, assetType)
	if err != nil {
		return plugin.ErrorResponse(err)
	}
	return plugin.SuccessResponse(records)
}

func (p *BinancePlugin) handleOHLCVStream(params map[string]any) plugin.Response {
	streamParams := ex.OHLCVStreamParamsFromMap(params)
	resolvedSymbol := strings.TrimSpace(streamParams.Market.Symbol)

	if resolvedSymbol == "" {
		return plugin.ErrorResponseMsg("market.symbol is required")
	}

	assetType := strings.TrimSpace(string(streamParams.Market.AssetType))

	streamReq := plugin.StreamSetupRequest{
		StreamID:   fmt.Sprintf("binance_ohlcv_%s_%s", resolvedSymbol, streamParams.Timeframe),
		StreamType: "ohlcv",
		Parameters: map[string]any{
			"market":    streamParams.Market,
			"timeframe": streamParams.Timeframe,
			"streamContext": map[string]any{
				"symbol":    resolvedSymbol,
				"timeframe": streamParams.Timeframe,
				"assetType": assetType,
			},
		},
	}

	setupResp, err := p.client.PrepareStream(streamReq)

	if err != nil {
		return plugin.ErrorResponse(err)
	}

	if !setupResp.Success {
		return plugin.ErrorResponseMsg(setupResp.Error)
	}

	marker := commonstream.StreamMarker{
		Stream:          true,
		StreamID:        streamReq.StreamID,
		WebSocketURL:    setupResp.WebSocketURL,
		Headers:         setupResp.Headers,
		Subprotocol:     setupResp.Subprotocol,
		InitialMessages: setupResp.InitialMessages,
		StreamContext:   setupResp.StreamContext,
	}
	if err := marker.Validate(); err != nil {
		return plugin.ErrorResponseMsg(err.Error())
	}

	return plugin.SuccessTypedResponse("StreamMarker", marker)
}

func shouldFetchScope(scopes []acct.ScopeType, scope acct.ScopeType) bool {
	if len(scopes) == 0 {
		return true
	}

	for _, s := range scopes {
		if s == scope {
			return true
		}
	}

	return false
}

func normalizeBinanceSide(side string) acct.OrderSide {
	s := strings.ToUpper(strings.TrimSpace(side))
	switch s {
	case "BUY":
		return acct.OrderSideBuy
	case "SELL":
		return acct.OrderSideSell
	case "":
		return acct.OrderSideUnknown
	default:
		return acct.OrderSide(strings.ToLower(s))
	}
}

func normalizeBinanceStatus(status string) acct.OrderStatus {
	s := strings.ToUpper(strings.TrimSpace(status))
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	switch s {
	case "NEW":
		return acct.OrderStatusNew
	case "PARTIALLY_FILLED", "PARTIAL_FILLED":
		return acct.OrderStatusPartiallyFilled
	case "FILLED":
		return acct.OrderStatusFilled
	case "CANCELED", "CANCELLED":
		return acct.OrderStatusCanceled
	case "REJECTED":
		return acct.OrderStatusRejected
	case "EXPIRED", "EXPIRED_IN_MATCH":
		return acct.OrderStatusExpired
	case "":
		return acct.OrderStatusUnknown
	default:
		return acct.OrderStatus(strings.ToLower(s))
	}
}

func normalizeBinanceTimeInForce(tif string) acct.TimeInForce {
	s := strings.ToUpper(strings.TrimSpace(tif))
	switch s {
	case "GTC":
		return acct.TimeInForceGTC
	case "IOC":
		return acct.TimeInForceIOC
	case "FOK":
		return acct.TimeInForceFOK
	case "GTX":
		return acct.TimeInForceGTX
	case "DAY":
		return acct.TimeInForceDay
	case "GTD":
		return acct.TimeInForceGTD
	case "":
		return acct.TimeInForceUnknown
	default:
		return acct.TimeInForce(strings.ToLower(s))
	}
}

func normalizeBinanceOrderType(orderType string) (acct.OrderType, bool) {
	s := strings.ToUpper(strings.TrimSpace(orderType))
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	switch s {
	case "LIMIT":
		return acct.OrderTypeLimit, false
	case "LIMIT_MAKER":
		return acct.OrderTypeLimit, true
	case "MARKET":
		return acct.OrderTypeMarket, false
	case "STOP", "STOP_LOSS":
		return acct.OrderTypeStop, false
	case "STOP_LIMIT", "STOP_LOSS_LIMIT":
		return acct.OrderTypeStopLimit, false
	case "STOP_MARKET":
		return acct.OrderTypeStopMarket, false
	case "TAKE_PROFIT":
		return acct.OrderTypeTakeProfit, false
	case "TAKE_PROFIT_LIMIT":
		return acct.OrderTypeTakeProfitLimit, false
	case "TAKE_PROFIT_MARKET":
		return acct.OrderTypeTakeProfitMarket, false
	case "TRAILING_STOP_MARKET", "TRAILING_STOP":
		return acct.OrderTypeTrailingStop, false
	case "":
		return acct.OrderTypeUnknown, false
	default:
		return acct.OrderType(strings.ToLower(s)), false
	}
}

func (p *BinancePlugin) handleGetAccount(params map[string]any) plugin.Response {
	_ = ex.GetAccountParamsFromMap(params)

	// TODO: implement Binance balances normalization once balance endpoints are wired.
	// We keep a minimal response so downstream callers can rely on the common shape.
	acctResp := acct.Account{
		Exchange:     "binance",
		Type:         acct.AccountTypeSpot,
		CustodyModel: acct.CustodyModelExchangeCustody,
		IsCustodial:  true,
		FetchedAt:    time.Now(),
		Scopes:       map[acct.ScopeType]acct.BalanceScope{},
		Extra:        map[string]any{},
	}

	return plugin.SuccessResponse(acctResp)
}

func (p *BinancePlugin) handleGetBalances(params map[string]any) plugin.Response {
	if p.client == nil {
		return plugin.ErrorResponseMsg("client not initialized")
	}

	parsed := ex.GetBalancesParamsFromMap(params)
	scopesExplicit := len(parsed.Scopes) > 0

	resp := acct.BalancesResponse{
		FetchedAt: time.Now(),
		Scopes:    map[acct.ScopeType]acct.BalanceScope{},
		Extra:     map[string]any{},
	}

	if shouldFetchScope(parsed.Scopes, acct.ScopeSpot) {
		balances, err := p.client.GetBalances(tt.AssetTypeSpot)
		if err != nil {
			return plugin.ErrorResponse(err)
		}

		scope := acct.BalanceScope{Balances: map[string]acct.AssetBalance{}, Extra: map[string]any{}}

		for _, b := range balances {
			asset := strings.ToUpper(strings.TrimSpace(b.Asset))
			if asset == "" {
				continue
			}

			total := ""
			if b.Free != "" || b.Locked != "" {
				freeVal, _ := strconv.ParseFloat(b.Free, 64)
				lockedVal, _ := strconv.ParseFloat(b.Locked, 64)
				total = fmt.Sprintf("%.15g", freeVal+lockedVal)
			}

			bal := acct.AssetBalance{
				Asset:            asset,
				Total:            total,
				AvailableToTrade: b.Free,
				Components:       map[string]string{},
				Extra:            map[string]any{},
			}

			if b.Locked != "" {
				bal.Components["locked"] = b.Locked
			}

			scope.Balances[asset] = bal
		}

		resp.Scopes[acct.ScopeSpot] = scope
	}

	if shouldFetchScope(parsed.Scopes, acct.ScopeFutures) {
		balances, err := p.client.GetFuturesBalances()
		if err != nil {
			if scopesExplicit {
				return plugin.ErrorResponse(err)
			}
			resp.Extra["futures_error"] = err.Error()
			return plugin.SuccessResponse(resp)
		}
		scope := acct.BalanceScope{Balances: map[string]acct.AssetBalance{}, Extra: map[string]any{}}
		for _, b := range balances {
			asset := strings.ToUpper(strings.TrimSpace(b.Asset))
			if asset == "" {
				continue
			}
			bal := acct.AssetBalance{
				Asset:            asset,
				Total:            b.Balance,
				AvailableToTrade: b.AvailableBalance,
				Components:       map[string]string{},
				Extra:            map[string]any{},
			}
			if b.CrossWalletBalance != "" {
				bal.Components["cross_wallet_balance"] = b.CrossWalletBalance
			}
			if b.CrossUnPnl != "" {
				bal.Components["cross_unrealized_pnl"] = b.CrossUnPnl
			}
			if b.MaxWithdrawAmount != "" {
				bal.Components["max_withdraw_amount"] = b.MaxWithdrawAmount
			}
			scope.Balances[asset] = bal
		}
		resp.Scopes[acct.ScopeFutures] = scope
	}

	return plugin.SuccessResponse(resp)
}

func (p *BinancePlugin) handleGetPositions(params map[string]any) plugin.Response {
	if p.client == nil {
		return plugin.ErrorResponseMsg("client not initialized")
	}

	parsed := ex.GetPositionsParamsFromMap(params)
	scopesExplicit := len(parsed.Scopes) > 0

	resp := acct.PositionsResponse{
		FetchedAt: time.Now(),
		Scopes:    map[acct.ScopeType]acct.PositionScope{},
		Extra:     map[string]any{},
	}

	if shouldFetchScope(parsed.Scopes, acct.ScopeFutures) {
		rows, err := p.client.GetFuturesPositions()
		if err != nil {
			if scopesExplicit {
				return plugin.ErrorResponse(err)
			}

			resp.Extra["futures_error"] = err.Error()
			return plugin.SuccessResponse(resp)
		}

		positions := make([]acct.Position, 0, len(rows))

		for _, pos := range rows {
			amt := pos.PositionAmt
			amtFloat, _ := strconv.ParseFloat(amt, 64)

			if amtFloat == 0 {
				continue
			}

			side := strings.ToUpper(strings.TrimSpace(pos.PositionSide))
			if side == "" {
				if amtFloat > 0 {
					side = "LONG"
				} else if amtFloat < 0 {
					side = "SHORT"
				}
			}

			qty := amt
			if amtFloat < 0 {
				qty = fmt.Sprintf("%.15g", -amtFloat)
			}

			leverage := 0
			if pos.Leverage != "" {
				if lv, err := strconv.Atoi(pos.Leverage); err == nil {
					leverage = lv
				}
			}

			positions = append(positions, acct.Position{
				Symbol:           pos.Symbol,
				Side:             strings.ToLower(side),
				Quantity:         qty,
				EntryPrice:       pos.EntryPrice,
				MarkPrice:        pos.MarkPrice,
				UnrealizedPnL:    pos.UnRealizedProfit,
				LiquidationPrice: pos.LiquidationPrice,
				Leverage:         leverage,
				IsIsolated:       strings.EqualFold(pos.MarginType, "isolated"),
				Components: map[string]string{
					"notional":        pos.Notional,
					"isolated_margin": pos.IsolatedMargin,
					"margin_type":     pos.MarginType,
				},
				Extra: map[string]any{},
			})
		}

		resp.Scopes[acct.ScopeFutures] = acct.PositionScope{
			Positions: positions,
			Extra:     map[string]any{},
		}
	}

	return plugin.SuccessResponse(resp)
}

func (p *BinancePlugin) handleGetOrders(params map[string]any) plugin.Response {
	if p.client == nil {
		return plugin.ErrorResponseMsg("client not initialized")
	}

	parsed := ex.GetOrdersParamsFromMap(params)

	symbol := ""
	if v, ok := params["symbol"].(string); ok {
		symbol = strings.TrimSpace(v)
	}

	if symbol == "" {
		if v, ok := params["market"].(map[string]any); ok {
			if s, ok := v["symbol"].(string); ok {
				symbol = strings.TrimSpace(s)
			}
		}
	}

	resp := acct.OrdersResponse{
		FetchedAt: time.Now(),
		Scopes:    map[acct.ScopeType]acct.OrderScope{},
		Extra:     map[string]any{},
	}

	spotOpenOnly := parsed.OpenOnly
	if !spotOpenOnly && symbol == "" {
		spotOpenOnly = true
	}

	spotRows, err := p.client.GetSpotOrders(symbol, spotOpenOnly)
	if err != nil {
		return plugin.ErrorResponse(err)
	}

	spotOrders := make([]acct.Order, 0, len(spotRows))

	for _, o := range spotRows {
		spotOrders = append(spotOrders, mapSpotOrder(o))
	}

	if len(spotOrders) > 0 {
		resp.Scopes[acct.ScopeSpot] = acct.OrderScope{
			Orders: spotOrders,
			Extra:  map[string]any{},
		}
	}

	futuresOpenOnly := parsed.OpenOnly
	if !futuresOpenOnly && symbol == "" {
		futuresOpenOnly = true
	}

	futRows, err := p.client.GetFuturesOrders(symbol, futuresOpenOnly)

	if err != nil {
		resp.Extra["futures_error"] = err.Error()
		return plugin.SuccessResponse(resp)
	}

	futOrders := make([]acct.Order, 0, len(futRows))

	for _, o := range futRows {
		futOrders = append(futOrders, mapFuturesOrder(o))
	}

	if len(futOrders) > 0 {
		resp.Scopes[acct.ScopeFutures] = acct.OrderScope{
			Orders: futOrders,
			Extra:  map[string]any{},
		}
	}

	return plugin.SuccessResponse(resp)
}

// firstNonEmptyString returns the first argument that is not empty; trailing
// stop orders carry their activation price where stops carry stopPrice.
func firstNonEmptyString(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// mapSpotOrder normalizes one spot venue order row onto the contract's
// acct.Order. Venue "0" prices mean "absent" and become "" (venuePrice).
func mapSpotOrder(o binance.SpotOrder) acct.Order {
	orderType, postOnly := normalizeBinanceOrderType(o.Type)
	remaining := ""

	if o.OrigQty != "" && o.ExecutedQty != "" {
		if q, err := strconv.ParseFloat(o.OrigQty, 64); err == nil {
			if exq, err := strconv.ParseFloat(o.ExecutedQty, 64); err == nil {
				remaining = fmt.Sprintf("%.15g", q-exq)
			}
		}
	}

	order := acct.Order{
		OrderID:           fmt.Sprintf("%d", o.OrderID),
		ClientOrderID:     o.ClientOrderID,
		Symbol:            o.Symbol,
		Side:              normalizeBinanceSide(o.Side),
		Type:              orderType,
		Status:            normalizeBinanceStatus(o.Status),
		Price:             venuePrice(o.Price),
		Quantity:          o.OrigQty,
		ExecutedQuantity:  o.ExecutedQty,
		RemainingQuantity: remaining,
		TimeInForce:       normalizeBinanceTimeInForce(o.TimeInForce),
		PostOnly:          postOnly,
		TriggerPrice:      venuePrice(o.StopPrice),
		Components: map[string]string{
			"cummulative_quote_qty": o.CummulativeQuoteQty,
			"orig_quote_qty":        o.OrigQuoteOrderQty,
		},
		Extra: map[string]any{},
	}

	if o.Time > 0 {
		order.CreatedAt = time.UnixMilli(o.Time)
	}

	if o.UpdateTime > 0 {
		order.UpdatedAt = time.UnixMilli(o.UpdateTime)
	}

	return order
}

// mapFuturesOrder normalizes one USDM-futures venue order row onto the
// contract's acct.Order. Venue "0" prices mean "absent" and become ""
// (venuePrice); a trailing stop reports its activation in activatePrice where
// plain stops carry stopPrice, so both are normalized before picking.
func mapFuturesOrder(o binance.FuturesOrder) acct.Order {
	orderType, postOnly := normalizeBinanceOrderType(o.Type)
	remaining := ""

	if o.OrigQty != "" && o.ExecutedQty != "" {
		if q, err := strconv.ParseFloat(o.OrigQty, 64); err == nil {
			if exq, err := strconv.ParseFloat(o.ExecutedQty, 64); err == nil {
				remaining = fmt.Sprintf("%.15g", q-exq)
			}
		}
	}

	order := acct.Order{
		OrderID:           fmt.Sprintf("%d", o.OrderID),
		ClientOrderID:     o.ClientOrderID,
		Symbol:            o.Symbol,
		Side:              normalizeBinanceSide(o.Side),
		Type:              orderType,
		Status:            normalizeBinanceStatus(o.Status),
		Price:             venuePrice(o.Price),
		Quantity:          o.OrigQty,
		ExecutedQuantity:  o.ExecutedQty,
		RemainingQuantity: remaining,
		TimeInForce:       normalizeBinanceTimeInForce(o.TimeInForce),
		ReduceOnly:        o.ReduceOnly,
		PostOnly:          postOnly,
		TriggerPrice:      firstNonEmptyString(venuePrice(o.StopPrice), venuePrice(o.ActivatePrice)),
		TriggerSource:     normalizeBinanceWorkingType(o.WorkingType),
		Components: map[string]string{
			"position_side":  o.PositionSide,
			"cum_quote":      o.CumQuote,
			"activate_price": o.ActivatePrice,
			"price_rate":     o.PriceRate,
		},
		Extra: map[string]any{},
	}

	if o.Time > 0 {
		order.CreatedAt = time.UnixMilli(o.Time)
	}

	if o.UpdateTime > 0 {
		order.UpdatedAt = time.UnixMilli(o.UpdateTime)
	}

	return order
}

// venuePrice maps a venue price field onto the contract's absent-price value.
// Binance reports "no price" as "0"/"0.00000000" (a plain limit order's
// stopPrice, a stop-market order's price); the contract represents an absent
// price as the empty string, never "0".
func venuePrice(s string) string {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return ""
	}
	if v, err := strconv.ParseFloat(trimmed, 64); err == nil && v == 0 {
		return ""
	}
	return s
}

// normalizeBinanceWorkingType maps Binance futures workingType onto the
// contract's TriggerSource; empty stays empty (venue default = CONTRACT_PRICE).
func normalizeBinanceWorkingType(t string) acct.TriggerSource {
	switch strings.ToUpper(strings.TrimSpace(t)) {
	case "MARK_PRICE":
		return acct.TriggerSourceMark
	case "CONTRACT_PRICE":
		return acct.TriggerSourceLast
	default:
		return ""
	}
}

// ============================================================================
// Stream Handler Wrapper
// ============================================================================

// streamHandlerWrapper wraps the plugin to implement the StreamHandler interface
type streamHandlerWrapper struct {
	p *BinancePlugin
}

func (w *streamHandlerWrapper) HandleStreamMessage(req plugin.StreamMessageRequest) (plugin.StreamMessageResponse, error) {
	if w.p.client == nil {
		return plugin.StreamMessageResponse{Success: false, Action: "ignore"}, fmt.Errorf("client not initialized")
	}
	return w.p.client.HandleStreamMessage(req)
}

func (w *streamHandlerWrapper) HandleConnectionEvent(event plugin.StreamConnectionEvent) (plugin.StreamConnectionResponse, error) {
	if w.p.client == nil {
		return plugin.StreamConnectionResponse{Success: false, Action: "ignore"}, fmt.Errorf("client not initialized")
	}
	return w.p.client.HandleConnectionEvent(event)
}
