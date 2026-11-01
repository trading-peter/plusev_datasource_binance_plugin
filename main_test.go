package main

import (
	"testing"
	"time"

	acct "github.com/plusev-terminal/go-plugin-common/v2/datasrc/exchange/account"
	m "github.com/plusev-terminal/go-plugin-common/v2/meta"

	"github.com/trading-peter/plusev_datasource_binance_plugin/binance"
)

// TestRateLimitDeclarations pins the manifest rate-limit table (hosts, limits,
// windows, scopes, weighted flag, paths, trading reserve) so the declared
// Binance budgets cannot silently drift. Sources: see the comment block above
// spotRateLimits/futuresRateLimits in main.go.
func TestRateLimitDeclarations(t *testing.T) {
	targets := (&BinancePlugin{}).GetMeta().Resources.AllowedNetworkTargets
	if len(targets) != 4 {
		t.Fatalf("expected 4 network targets (spot REST, futures REST, spot WS, futures WS), got %d", len(targets))
	}

	byPattern := map[string]m.NetworkTargetRule{}
	for _, tgt := range targets {
		byPattern[tgt.Pattern] = tgt
	}

	if ws := byPattern["wss://fstream.binance.com/*"]; len(ws.Limits) != 0 {
		t.Errorf("futures ws target must not carry limits, got %d", len(ws.Limits))
	}
	if ws := byPattern["wss://stream.binance.com:9443/*"]; len(ws.Limits) != 0 {
		t.Errorf("spot ws target must not carry limits, got %d", len(ws.Limits))
	}

	type wantRule struct {
		id       string
		limit    int
		window   time.Duration
		scope    m.RateLimitScope
		weighted bool
		paths    []string
	}

	cases := []struct {
		pattern string
		reserve float64
		rules   []wantRule
	}{
		{
			pattern: "https://api.binance.com/*",
			reserve: 0.3,
			rules: []wantRule{
				{id: "weight", limit: 6000, window: time.Minute, weighted: true},
				{id: "raw", limit: 300000, window: 5 * time.Minute},
				{id: "orders-10s", limit: 100, window: 10 * time.Second, scope: m.RateLimitScopeCredential, paths: []string{"/api/v3/order"}},
				{id: "orders-24h", limit: 200000, window: 24 * time.Hour, scope: m.RateLimitScopeCredential, paths: []string{"/api/v3/order"}},
			},
		},
		{
			pattern: "https://fapi.binance.com/*",
			reserve: 0.3,
			rules: []wantRule{
				{id: "weight", limit: 2400, window: time.Minute, weighted: true},
				{id: "orders-10s", limit: 300, window: 10 * time.Second, scope: m.RateLimitScopeCredential, paths: []string{"/fapi/v1/order", "/fapi/v1/batchOrders"}},
				{id: "orders-1m", limit: 1200, window: time.Minute, scope: m.RateLimitScopeCredential, paths: []string{"/fapi/v1/order", "/fapi/v1/batchOrders"}},
			},
		},
	}

	for _, tc := range cases {
		tgt, ok := byPattern[tc.pattern]
		if !ok {
			t.Fatalf("missing network target %s", tc.pattern)
		}
		if tgt.TradingReserve == nil || *tgt.TradingReserve != tc.reserve {
			t.Errorf("%s: TradingReserve = %v, want %v", tc.pattern, tgt.TradingReserve, tc.reserve)
		}
		wantHdrs := []m.UsageHeader{
			{Prefix: "x-mbx-used-weight-", Name: "used-weight-", Field: m.UsageFieldUsed},
			{Prefix: "x-mbx-order-count-", Name: "order-count-", Field: m.UsageFieldUsed},
		}
		if len(tgt.UsageHeaders) != len(wantHdrs) {
			t.Fatalf("%s: got %d usage headers, want %d", tc.pattern, len(tgt.UsageHeaders), len(wantHdrs))
		}
		for i, wh := range wantHdrs {
			if tgt.UsageHeaders[i] != wh {
				t.Errorf("%s: usage header %d = %+v, want %+v", tc.pattern, i, tgt.UsageHeaders[i], wh)
			}
		}
		if len(tgt.Limits) != len(tc.rules) {
			t.Fatalf("%s: got %d limits, want %d", tc.pattern, len(tgt.Limits), len(tc.rules))
		}
		got := map[string]m.RateLimitRule{}
		for _, r := range tgt.Limits {
			got[r.ID] = r
		}
		for _, w := range tc.rules {
			r, ok := got[w.id]
			if !ok {
				t.Errorf("%s: missing rule %q", tc.pattern, w.id)
				continue
			}
			if r.Limit != w.limit {
				t.Errorf("%s/%s: Limit = %d, want %d", tc.pattern, w.id, r.Limit, w.limit)
			}
			if r.WindowMs != w.window.Milliseconds() {
				t.Errorf("%s/%s: WindowMs = %d, want %d", tc.pattern, w.id, r.WindowMs, w.window.Milliseconds())
			}
			if r.Burst != 0 {
				t.Errorf("%s/%s: Burst = %d, want 0 (default = limit, no published burst)", tc.pattern, w.id, r.Burst)
			}
			if r.Scope != w.scope {
				t.Errorf("%s/%s: Scope = %q, want %q", tc.pattern, w.id, r.Scope, w.scope)
			}
			if r.CostWeighted != w.weighted {
				t.Errorf("%s/%s: CostWeighted = %v, want %v", tc.pattern, w.id, r.CostWeighted, w.weighted)
			}
			if len(r.Paths) != len(w.paths) {
				t.Errorf("%s/%s: Paths = %v, want %v", tc.pattern, w.id, r.Paths, w.paths)
				continue
			}
			for i, p := range w.paths {
				if r.Paths[i] != p {
					t.Errorf("%s/%s: Paths[%d] = %q, want %q", tc.pattern, w.id, i, r.Paths[i], p)
				}
			}
		}
	}
}

// TestVenuePrice pins the absent-price rule: Binance reports "no price" as
// "0"/"0.00000000"; the contract's absent price is the empty string.
func TestVenuePrice(t *testing.T) {
	cases := map[string]string{
		"":             "",
		" ":            "",
		"0":            "",
		"0.0":          "",
		"0.00000000":   "",
		" 0.00000000 ": "",
		"65000.10":     "65000.10",
		"0.5":          "0.5",
		"n/a":          "n/a",
	}
	for in, want := range cases {
		if got := venuePrice(in); got != want {
			t.Errorf("venuePrice(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestMapSpotOrderAbsentPrices guards the zero-price bug on the spot mapping:
// a plain limit order (stopPrice "0.00000000") carries no TriggerPrice, a
// plain market order (price "0.00000000") carries no Price — never "0".
func TestMapSpotOrderAbsentPrices(t *testing.T) {
	limit := mapSpotOrder(binance.SpotOrder{
		OrderID: 1, Symbol: "BTCUSDT", Side: "BUY", Type: "LIMIT",
		Price: "65000.00", OrigQty: "1", ExecutedQty: "0",
		StopPrice: "0.00000000", Status: "NEW", TimeInForce: "GTC",
	})
	if limit.Price != "65000.00" {
		t.Errorf("limit.Price = %q, want 65000.00", limit.Price)
	}
	if limit.TimeInForce != acct.TimeInForceGTC {
		t.Errorf("limit.TimeInForce = %q, want %q", limit.TimeInForce, acct.TimeInForceGTC)
	}
	if limit.TriggerPrice != "" {
		t.Errorf("limit.TriggerPrice = %q, want empty (venue stopPrice \"0.00000000\" means absent)", limit.TriggerPrice)
	}

	market := mapSpotOrder(binance.SpotOrder{
		OrderID: 2, Symbol: "BTCUSDT", Side: "SELL", Type: "MARKET",
		Price: "0.00000000", OrigQty: "1", ExecutedQty: "1",
		StopPrice: "0", Status: "FILLED",
	})
	if market.Price != "" {
		t.Errorf("market.Price = %q, want empty (venue price \"0.00000000\" means absent)", market.Price)
	}
	if market.TriggerPrice != "" {
		t.Errorf("market.TriggerPrice = %q, want empty", market.TriggerPrice)
	}

	stop := mapSpotOrder(binance.SpotOrder{
		OrderID: 3, Symbol: "BTCUSDT", Side: "SELL", Type: "STOP_LOSS_LIMIT",
		Price: "64000.00", OrigQty: "1", ExecutedQty: "0",
		StopPrice: "63000.00", Status: "NEW", TimeInForce: "GTC",
	})
	if stop.TriggerPrice != "63000.00" {
		t.Errorf("stop.TriggerPrice = %q, want 63000.00 (real triggers survive normalization)", stop.TriggerPrice)
	}
}

// TestMapFuturesOrderAbsentPrices guards the zero-price bug on the USDM
// mapping: a plain futures limit order (stopPrice "0") carries no
// TriggerPrice; a stop-market order reports price "0" with the real trigger
// in stopPrice; a trailing stop reports stopPrice "0" with its activation in
// activatePrice.
func TestMapFuturesOrderAbsentPrices(t *testing.T) {
	limit := mapFuturesOrder(binance.FuturesOrder{
		OrderID: 1, Symbol: "BTCUSDT", Side: "BUY", Type: "LIMIT",
		Price: "65000.00", OrigQty: "1", ExecutedQty: "0",
		StopPrice: "0", ActivatePrice: "0", Status: "NEW", TimeInForce: "GTC",
		ReduceOnly: true,
	})
	if limit.Price != "65000.00" {
		t.Errorf("limit.Price = %q, want 65000.00", limit.Price)
	}
	if limit.TimeInForce != acct.TimeInForceGTC {
		t.Errorf("limit.TimeInForce = %q, want %q", limit.TimeInForce, acct.TimeInForceGTC)
	}
	if !limit.ReduceOnly {
		t.Error("limit.ReduceOnly = false, want true (venue reduceOnly must carry through)")
	}
	if limit.TriggerPrice != "" {
		t.Errorf("limit.TriggerPrice = %q, want empty (venue stopPrice \"0\" means absent)", limit.TriggerPrice)
	}

	stopMarket := mapFuturesOrder(binance.FuturesOrder{
		OrderID: 2, Symbol: "BTCUSDT", Side: "SELL", Type: "STOP_MARKET",
		Price: "0", OrigQty: "1", ExecutedQty: "0",
		StopPrice: "61000.00", Status: "NEW", WorkingType: "MARK_PRICE",
	})
	if stopMarket.Price != "" {
		t.Errorf("stopMarket.Price = %q, want empty (venue price \"0\" means absent)", stopMarket.Price)
	}
	if stopMarket.TriggerPrice != "61000.00" {
		t.Errorf("stopMarket.TriggerPrice = %q, want 61000.00 (the real trigger)", stopMarket.TriggerPrice)
	}

	trailing := mapFuturesOrder(binance.FuturesOrder{
		OrderID: 3, Symbol: "BTCUSDT", Side: "SELL", Type: "TRAILING_STOP_MARKET",
		Price: "0", OrigQty: "1", ExecutedQty: "0",
		StopPrice: "0", ActivatePrice: "42100.10", PriceRate: "0.4",
		Status: "NEW", WorkingType: "CONTRACT_PRICE",
	})
	if trailing.TriggerPrice != "42100.10" {
		t.Errorf("trailing.TriggerPrice = %q, want 42100.10 (activation price, not the \"0\" stopPrice)", trailing.TriggerPrice)
	}
	if stopMarket.TriggerSource != acct.TriggerSourceMark {
		t.Errorf("stopMarket.TriggerSource = %q, want %q (MARK_PRICE)", stopMarket.TriggerSource, acct.TriggerSourceMark)
	}
	if trailing.TriggerSource != acct.TriggerSourceLast {
		t.Errorf("trailing.TriggerSource = %q, want %q (CONTRACT_PRICE)", trailing.TriggerSource, acct.TriggerSourceLast)
	}
}

// TestGetRateLimitsOptsOutOfLegacyLayer guards the explicit opt-out: manifest
// limits must stay the only enforced layer.
func TestGetRateLimitsOptsOutOfLegacyLayer(t *testing.T) {
	limits := (&BinancePlugin{}).GetRateLimits()
	if limits == nil {
		t.Fatal("GetRateLimits must return an empty slice, not nil")
	}
	if len(limits) != 0 {
		t.Fatalf("GetRateLimits = %v, want empty", limits)
	}
}
