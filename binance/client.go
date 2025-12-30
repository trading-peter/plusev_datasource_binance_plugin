package binance

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	ex "github.com/plusev-terminal/go-plugin-common/datasrc/exchange"
	"github.com/plusev-terminal/go-plugin-common/logging"
	"github.com/plusev-terminal/go-plugin-common/plugin"
	"github.com/plusev-terminal/go-plugin-common/requester"
	rt "github.com/plusev-terminal/go-plugin-common/requester/types"
	tt "github.com/plusev-terminal/go-plugin-common/trading"
	"github.com/plusev-terminal/go-plugin-common/utils"
)

type Client struct {
	log         *logging.Logger
	requester   rt.RequestDoer
	apiKey      string
	apiSecret   string
	spotBaseURL string
	umBaseURL   string
}

func NewClient(req *requester.Requester, baseURL string) *Client {
	return &Client{
		log:         logging.NewLogger("binance-datasource"),
		requester:   req,
		spotBaseURL: baseURL,
		umBaseURL:   "https://fapi.binance.com",
	}
}

func (c *Client) SetCredentials(creds map[string]string) {
	c.apiKey = creds["apiKey"]
	c.apiSecret = creds["apiSecret"]
}

func (c *Client) GetConfigFields() []plugin.ConfigField {
	return []plugin.ConfigField{
		{
			Name:        "apiKey",
			Label:       "API Key",
			Type:        "text",
			Required:    false,
			Encrypt:     true,
			Mask:        true,
			Placeholder: "Your Binance API key",
			Description: "Binance API key (optional for public market data)",
		},
		{
			Name:        "apiSecret",
			Label:       "API Secret",
			Type:        "password",
			Required:    false,
			Encrypt:     true,
			Mask:        true,
			Placeholder: "Your Binance API secret",
			Description: "Binance API secret (optional for public market data)",
		},
	}
}

// GetMarkets returns available trading pairs
func (c *Client) GetMarkets() ([]tt.Market, error) {
	spotInfo, err := c.getSpotExchangeInfo()
	if err != nil {
		return nil, err
	}
	// futuresInfo, err := c.getFuturesExchangeInfo()
	// if err != nil {
	// 	return nil, err
	// }

	markets := make([]tt.Market, 0, len(spotInfo.Symbols))
	for _, s := range spotInfo.Symbols {
		if s.Status != "TRADING" {
			continue
		}

		markets = append(markets, parseBinanceSymbolToMarketInfo(s, "spot"))
	}

	// for _, s := range futuresInfo.Symbols {
	// 	if s.Status != "TRADING" {
	// 		continue
	// 	}

	// 	markets = append(markets, tt.Market{
	// 		Symbol:    s.Symbol,
	// 		Base:      s.BaseAsset,
	// 		Quote:     s.QuoteAsset,
	// 		AssetType: "futures",
	// 	})
	// }

	return markets, nil
}

// parseBinanceSymbolToMarketInfo converts one Binance symbol to tt.Market
func parseBinanceSymbolToMarketInfo(sym binanceSymbol, assetType string) tt.Market {
	mi := tt.Market{
		Symbol:    sym.Symbol,
		Base:      sym.BaseAsset,
		Quote:     sym.QuoteAsset,
		AssetType: assetType, // caller sets "spot", "perpetual", etc.
		Status:    sym.Status,
		Label:     fmt.Sprintf("%s/%s", sym.BaseAsset, sym.QuoteAsset),
	}

	// Process filters
	for _, f := range sym.Filters {
		switch f.FilterType {
		case "PRICE_FILTER":
			if f.TickSize != "" {
				mi.PriceTick = f.TickSize
			}
		case "LOT_SIZE":
			if f.StepSize != "" {
				mi.QuantityTick = f.StepSize
			}
			if f.MinQty != "" {
				mi.MinQuantity = f.MinQty
			}
			if f.MaxQty != "" {
				mi.MaxQuantity = f.MaxQty
			}
		case "MIN_NOTIONAL":
			if f.MinNotional != "" {
				mi.MinNotional = f.MinNotional
			} else if f.Notional != "" {
				mi.MinNotional = f.Notional
			}
		case "PERCENT_PRICE":
			// optional: multiplierUp / multiplierDown for price bands
		case "MARKET_LOT_SIZE":
			// similar to LOT_SIZE but for market orders
		}
	}

	// Futures-specific handling
	if sym.ContractSize != "" {
		mi.ContractSize = sym.ContractSize
	}
	// For linear USDT-margined perps, contractSize is usually "1" (implied)
	if assetType == "perpetual" && mi.ContractSize == "" {
		mi.ContractSize = "1"
	}

	// Optional: derive precision for UI
	if mi.PriceTick != "" {
		mi.PricePrecision = countDecimalPlaces(mi.PriceTick)
	}
	if mi.QuantityTick != "" {
		mi.QuantityPrecision = countDecimalPlaces(mi.QuantityTick)
	}

	return mi
}

// Helper to count significant decimal places (e.g., "0.00100000" → 3).
// Keeps allocations near-zero and handles scientific notation if it appears.
func countDecimalPlaces(s string) int {
	if s == "" || s == "0" {
		return 0
	}

	// Handle scientific notation defensively.
	if strings.ContainsAny(s, "eE") {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			// Use fixed format with enough precision for tick sizes.
			s = strconv.FormatFloat(f, 'f', 18, 64)
		}
	}

	i := strings.IndexByte(s, '.')
	if i < 0 {
		return 0
	}

	// Trim trailing zeros after the decimal point.
	j := len(s) - 1
	for j > i && s[j] == '0' {
		j--
	}
	if j == i {
		return 0
	}

	prec := j - i
	if prec > 12 {
		return 12
	}
	return prec
}

// GetTimeframes returns supported timeframes
func (c *Client) GetTimeframes() []tt.Timeframe {
	return []tt.Timeframe{
		{Value: 1, Unit: tt.Minutes},
		{Value: 3, Unit: tt.Minutes},
		{Value: 5, Unit: tt.Minutes},
		{Value: 15, Unit: tt.Minutes},
		{Value: 30, Unit: tt.Minutes},
		{Value: 1, Unit: tt.Hours},
		{Value: 2, Unit: tt.Hours},
		{Value: 4, Unit: tt.Hours},
		{Value: 6, Unit: tt.Hours},
		{Value: 8, Unit: tt.Hours},
		{Value: 12, Unit: tt.Hours},
		{Value: 1, Unit: tt.Days},
		{Value: 3, Unit: tt.Days},
		{Value: 1, Unit: tt.Weeks},
		{Value: 1, Unit: tt.Months},
	}
}

// GetOHLCV fetches historical OHLCV data
func (c *Client) GetOHLCV(req ex.GetOHLCVParams) ([]tt.OHLCVRecord, error) {
	return c.GetOHLCVWithAssetType(req, "spot")
}

func (c *Client) GetOHLCVWithAssetType(req ex.GetOHLCVParams, assetType string) ([]tt.OHLCVRecord, error) {
	if assetType == "" {
		assetType = "spot"
	}

	interval := c.toBinanceInterval(req.Timeframe)

	if interval == "" {
		return nil, fmt.Errorf("unsupported timeframe: %s", req.Timeframe)
	}

	symbol := strings.TrimSpace(req.Market.Symbol)

	if symbol == "" {
		return nil, fmt.Errorf("market.symbol is required")
	}

	return c.getKlines(assetType, c.toBinanceSymbol(symbol), interval, req.StartTime, req.EndTime, req.Limit)
}

// PrepareStream prepares streaming connection setup
func (c *Client) PrepareStream(req plugin.StreamSetupRequest) (plugin.StreamSetupResponse, error) {
	// Caller-owned context (required).
	streamContext, _ := req.Parameters["streamContext"].(map[string]any)
	if streamContext == nil {
		c.log.InfoWithData("PrepareStream: missing streamContext", map[string]any{
			"streamID":   req.StreamID,
			"streamType": req.StreamType,
			"paramKeys":  keysOfAnyMap(req.Parameters),
		})
		return plugin.StreamSetupResponse{Success: false, Error: "streamContext is required"}, nil
	}

	ctxSymbol, _ := streamContext["symbol"].(string)
	ctxTimeframe, _ := streamContext["timeframe"].(string)
	ctxAssetType, _ := streamContext["assetType"].(string)
	ctxSymbol = strings.TrimSpace(ctxSymbol)
	ctxTimeframe = strings.TrimSpace(ctxTimeframe)
	ctxAssetType = strings.TrimSpace(ctxAssetType)
	if ctxAssetType == "" {
		ctxAssetType = "spot"
	}

	// Log what we got (helps debug host->plugin param encoding).
	c.log.InfoWithData("PrepareStream: received parameters", map[string]any{
		"streamID":        req.StreamID,
		"streamType":      req.StreamType,
		"paramKeys":       keysOfAnyMap(req.Parameters),
		"marketParamType": fmt.Sprintf("%T", req.Parameters["market"]),
		"timeframeParam":  req.Parameters["timeframe"],
		"ctxSymbol":       ctxSymbol,
		"ctxTimeframe":    ctxTimeframe,
		"ctxAssetType":    ctxAssetType,
	})

	if ctxSymbol == "" || ctxTimeframe == "" {
		return plugin.StreamSetupResponse{Success: false, Error: "streamContext.symbol and streamContext.timeframe are required"}, nil
	}

	interval := c.toBinanceInterval(ctxTimeframe)

	if interval == "" {
		return plugin.StreamSetupResponse{Success: false, Error: "unsupported timeframe"}, nil
	}

	// Binance WebSocket URL
	// We use the combined stream URL pattern or single stream
	// For simplicity, let's use the raw stream URL and send subscription
	wsURL := "wss://stream.binance.com:9443/ws"

	if ctxAssetType == "futures" {
		wsURL = "wss://fstream.binance.com/ws"
	}

	// Subscription message
	streamName := fmt.Sprintf("%s@kline_%s", strings.ToLower(c.toBinanceSymbol(ctxSymbol)), interval)
	subMsg := map[string]any{
		"method": "SUBSCRIBE",
		"params": []string{streamName},
		"id":     time.Now().Unix(),
	}

	subMsgBytes, _ := json.Marshal(subMsg)

	return plugin.StreamSetupResponse{
		Success:      true,
		WebSocketURL: wsURL,
		InitialMessages: []string{
			string(subMsgBytes),
		},
		StreamContext: streamContext,
	}, nil
}

func keysOfAnyMap(m map[string]any) []string {
	if m == nil {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// HandleStreamMessage handles incoming WebSocket messages
func (c *Client) HandleStreamMessage(msg plugin.StreamMessageRequest) (plugin.StreamMessageResponse, error) {
	// Parse message
	// Binance kline stream format:
	// {
	//   "e": "kline",
	//   "E": 123456789,
	//   "s": "BNBBTC",
	//   "k": { ... }
	// }

	// Parse strategy:
	// 1) Unquote once if msg.Message itself is a JSON string containing JSON.
	// 2) Decode a small routing envelope to decide which full struct to unmarshal.
	// This keeps it easy to add more message types later.

	type klineEvent struct {
		Event  string `json:"e"`
		Symbol string `json:"s"`
		Kline  struct {
			StartTime int64  `json:"t"`
			CloseTime int64  `json:"T"`
			Symbol    string `json:"s"`
			Interval  string `json:"i"`
			Open      string `json:"o"`
			Close     string `json:"c"`
			High      string `json:"h"`
			Low       string `json:"l"`
			Volume    string `json:"v"`
			IsFinal   bool   `json:"x"`
		} `json:"k"`
	}

	var env map[string]any
	if err := json.Unmarshal(msg.Message, &env); err != nil {
		c.log.ErrorWithData("HandleStreamMessage: failed to unmarshal envelope", map[string]any{
			"error":   err.Error(),
			"message": string(msg.Message),
		})
		return plugin.IgnoreResponse(), nil
	}

	eventType := utils.GetValue("e", env, "")

	if eventType != "kline" {
		return plugin.IgnoreResponse(), nil
	}

	kline := utils.ExtractMap("k", env)
	if kline == nil {
		c.log.ErrorWithData("HandleStreamMessage: missing kline data", map[string]any{
			"message": string(msg.Message),
		})
		return plugin.IgnoreResponse(), nil
	}

	openTime := utils.ExtractTime("t", kline).UTC().Unix()
	open := utils.GetValue("o", kline, "0")
	high := utils.GetValue("h", kline, "0")
	low := utils.GetValue("l", kline, "0")
	close := utils.GetValue("c", kline, "0")
	volume := utils.GetValue("v", kline, "0")

	// Convert to OHLCVRecord
	record := tt.OHLCVRecord{
		OpenTime: openTime,
		Open:     open,
		High:     high,
		Low:      low,
		Close:    close,
		Volume:   volume,
	}

	return plugin.StreamResponse("ohlcv", record), nil
}

// HandleConnectionEvent handles connection events
func (c *Client) HandleConnectionEvent(event plugin.StreamConnectionEvent) (plugin.StreamConnectionResponse, error) {
	c.log.InfoWithData("WebSocket connection event", map[string]any{"eventType": event.EventType, "error": event.Error})
	return plugin.DefaultConnectionEventHandler(event), nil
}

// Helper to convert PlusEV timeframe to Binance interval
func (c *Client) toBinanceInterval(tf string) string {
	supported := map[string]bool{
		"1m": true, "3m": true, "5m": true, "15m": true, "30m": true,
		"1h": true, "2h": true, "4h": true, "6h": true, "8h": true, "12h": true,
		"1d": true, "3d": true,
		"1w": true,
		"1M": true,
	}
	if supported[tf] {
		return tf
	}
	return ""
}

func (c *Client) toBinanceSymbol(symbol string) string {
	// PlusEV often uses BASE/QUOTE; Binance expects BASEQUOTE.
	return strings.ReplaceAll(strings.ToUpper(symbol), "/", "")
}

// ---- Private endpoints parity (account info, balances, orders) ----

type Balance struct {
	Asset  string `json:"asset"`
	Free   string `json:"free"`
	Locked string `json:"locked"`
}

type AccountInfo struct {
	CanTrade bool      `json:"canTrade"`
	Balances []Balance `json:"balances"`
}

func (c *Client) isAuthenticated() bool {
	return c.apiKey != "" && c.apiSecret != ""
}

func (c *Client) signedQuery(params map[string]string) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	vals := url.Values{}
	for _, k := range keys {
		vals.Set(k, params[k])
	}

	query := vals.Encode()
	mac := hmac.New(sha256.New, []byte(c.apiSecret))
	mac.Write([]byte(query))
	sig := hex.EncodeToString(mac.Sum(nil))
	return query + "&signature=" + sig
}

func (c *Client) GetAccountInfo(assetType string) (*AccountInfo, error) {
	if !c.isAuthenticated() {
		return nil, fmt.Errorf("apiKey/apiSecret required")
	}
	if assetType == "" {
		assetType = "spot"
	}

	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	path := "/api/v3/account"
	base := c.spotBaseURL
	if assetType == "futures" {
		path = "/fapi/v2/account"
		base = c.umBaseURL
	}

	query := c.signedQuery(map[string]string{"timestamp": ts})
	req := &rt.Request{
		Method: "GET",
		URL:    base + path + "?" + query,
		Headers: map[string]string{
			"X-MBX-APIKEY": c.apiKey,
		},
	}

	var resp AccountInfo
	_, err := c.requester.Send(req, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) GetBalances(assetType string) ([]Balance, error) {
	info, err := c.GetAccountInfo(assetType)
	if err != nil {
		return nil, err
	}
	// Futures /fapi/v2/account uses different schema; we keep spot balances parity here.
	return info.Balances, nil
}

// ---- helpers ----

// ---- REST helpers (requester-based) ----

// Binance filter structures
type binanceFilter struct {
	FilterType    string `json:"filterType"`
	MinPrice      string `json:"minPrice,omitempty"`
	MaxPrice      string `json:"maxPrice,omitempty"`
	TickSize      string `json:"tickSize,omitempty"`
	MinQty        string `json:"minQty,omitempty"`
	MaxQty        string `json:"maxQty,omitempty"`
	StepSize      string `json:"stepSize,omitempty"`
	MinNotional   string `json:"minNotional,omitempty"` // spot
	Notional      string `json:"notional,omitempty"`    // sometimes used instead
	ApplyToMarket bool   `json:"applyToMarket,omitempty"`
	Limit         int    `json:"limit,omitempty"`
	MaxNumOrders  int    `json:"maxNumOrders,omitempty"`
	// add more if needed
}

type binanceSymbol struct {
	Symbol              string          `json:"symbol"`
	Status              string          `json:"status"`
	BaseAsset           string          `json:"baseAsset"`
	BaseAssetPrecision  int             `json:"baseAssetPrecision"`
	QuoteAsset          string          `json:"quoteAsset"`
	QuotePrecision      int             `json:"quotePrecision"` // deprecated
	QuoteAssetPrecision int             `json:"quoteAssetPrecision"`
	OrderTypes          []string        `json:"orderTypes"`
	IcebergAllowed      bool            `json:"icebergAllowed"`
	OcoAllowed          bool            `json:"ocoAllowed"`
	Filters             []binanceFilter `json:"filters"`

	// Futures-specific fields (present in fapi/dapi)
	ContractType string `json:"contractType,omitempty"` // PERPETUAL, CURRENT_QUARTER etc.
	ContractSize string `json:"contractSize,omitempty"` // e.g. "100" for coin-margined
	DeliveryDate int64  `json:"deliveryDate,omitempty"` // for dated futures
}

type spotExchangeInfoResponse struct {
	Symbols []binanceSymbol `json:"symbols"`
}

func (c *Client) getSpotExchangeInfo() (*spotExchangeInfoResponse, error) {
	req := &rt.Request{Method: "GET", URL: c.spotBaseURL + "/api/v3/exchangeInfo?showPermissionSets=false", Headers: map[string]string{"Content-Type": "application/json"}}
	var resp spotExchangeInfoResponse
	_, err := c.requester.Send(req, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch spot exchangeInfo: %w", err)
	}
	return &resp, nil
}

type futuresExchangeInfoResponse struct {
	Symbols []struct {
		Symbol              string `json:"symbol"`
		Status              string `json:"status"`
		BaseAsset           string `json:"baseAsset"`
		QuoteAsset          string `json:"quoteAsset"`
		BaseAssetPrecision  int    `json:"baseAssetPrecision"`
		QuoteAssetPrecision int    `json:"quoteAssetPrecision"`
	} `json:"symbols"`
}

func (c *Client) getFuturesExchangeInfo() (*futuresExchangeInfoResponse, error) {
	req := &rt.Request{Method: "GET", URL: c.umBaseURL + "/fapi/v1/exchangeInfo", Headers: map[string]string{"Content-Type": "application/json"}}
	var resp futuresExchangeInfoResponse
	_, err := c.requester.Send(req, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch futures exchangeInfo: %w", err)
	}
	return &resp, nil
}

func (c *Client) getKlines(assetType, symbol, interval string, start, end *time.Time, limit int) ([]tt.OHLCVRecord, error) {
	base := c.spotBaseURL
	path := "/api/v3/klines"
	if assetType == "futures" {
		base = c.umBaseURL
		path = "/fapi/v1/klines"
	}

	q := url.Values{}
	q.Set("symbol", symbol)
	q.Set("interval", interval)
	if start != nil {
		q.Set("startTime", strconv.FormatInt(start.UnixMilli(), 10))
	}
	if end != nil {
		q.Set("endTime", strconv.FormatInt(end.UnixMilli(), 10))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}

	req := &rt.Request{Method: "GET", URL: base + path + "?" + q.Encode(), Headers: map[string]string{"Content-Type": "application/json"}}
	var raw [][]any
	_, err := c.requester.Send(req, &raw)
	if err != nil {
		return nil, err
	}

	records := make([]tt.OHLCVRecord, 0, len(raw))
	for _, row := range raw {
		// [0]=openTime(ms), [1]=open, [2]=high, [3]=low, [4]=close, [5]=volume
		if len(row) < 6 {
			continue
		}
		openTimeMs, _ := asInt64(row[0])
		records = append(records, tt.OHLCVRecord{
			OpenTime: openTimeMs / 1000,
			Open:     asString(row[1]),
			High:     asString(row[2]),
			Low:      asString(row[3]),
			Close:    asString(row[4]),
			Volume:   asString(row[5]),
		})
	}

	return records, nil
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case json.Number:
		return t.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func asInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case float64:
		return int64(t), true
	case int64:
		return t, true
	case json.Number:
		i, err := t.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}
