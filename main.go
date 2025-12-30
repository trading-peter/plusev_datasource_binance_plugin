package main

import (
	"fmt"
	"time"

	"strings"

	ex "github.com/plusev-terminal/go-plugin-common/datasrc/exchange"
	m "github.com/plusev-terminal/go-plugin-common/meta"
	"github.com/plusev-terminal/go-plugin-common/plugin"
	"github.com/plusev-terminal/go-plugin-common/requester"
	commonstream "github.com/plusev-terminal/go-plugin-common/stream"

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
		Version:     "v1.0.0",
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
				{Pattern: "https://api.binance.com/*"},
				{Pattern: "https://fapi.binance.com/*"},
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

func (p *BinancePlugin) GetRateLimits() []plugin.RateLimit {
	// Define rate limits based on Binance API documentation
	// Binance Spot API: 1200 requests per minute (IP limit)
	// Binance Futures API: 2400 requests per minute (IP limit)

	return []plugin.RateLimit{
		{
			Command: ex.CMD_GET_MARKETS,
			Scope:   []plugin.RateLimitScope{plugin.RateLimitScopeIP},
			RPS:     20.0,
			Burst:   20,
		},
		{
			Command: ex.CMD_GET_TIMEFRAMES,
			Scope:   []plugin.RateLimitScope{plugin.RateLimitScopeIP},
			RPS:     20.0,
			Burst:   20,
		},
		{
			Command: ex.CMD_GET_OHLCV,
			Scope:   []plugin.RateLimitScope{plugin.RateLimitScopeIP},
			RPS:     20.0,
			Burst:   20,
		},
		{
			Command: ex.CMD_OHLCV_STREAM,
			Scope:   []plugin.RateLimitScope{plugin.RateLimitScopeIP},
			RPS:     5.0,
			Burst:   20,
		},
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
	router.Register(ex.CMD_ACCOUNT_BALANCES, p.handleAccountBalances)
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
	assetType := "spot"
	if at := strings.TrimSpace(ohlcvParams.Market.AssetType); at != "" {
		assetType = at
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

	assetType := strings.TrimSpace(streamParams.Market.AssetType)

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

func (p *BinancePlugin) handleAccountBalances(params map[string]any) plugin.Response {
	abParams := ex.AccountBalancesParamsFromMap(params)
	assetType := "spot"
	if at := strings.TrimSpace(abParams.Market.AssetType); at != "" {
		assetType = at
	}
	balances, err := p.client.GetBalances(assetType)
	if err != nil {
		return plugin.ErrorResponse(err)
	}
	return plugin.SuccessResponse(balances)
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
