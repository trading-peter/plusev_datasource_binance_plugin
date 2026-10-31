package main

import (
	"testing"
	"time"

	m "github.com/plusev-terminal/go-plugin-common/meta"
)

// TestRateLimitDeclarations pins the manifest rate-limit table (hosts, limits,
// windows, scopes, weighted flag, paths, trading reserve) so the declared
// Binance budgets cannot silently drift. Sources: see the comment block above
// spotRateLimits/futuresRateLimits in main.go.
func TestRateLimitDeclarations(t *testing.T) {
	targets := (&BinancePlugin{}).GetMeta().Resources.AllowedNetworkTargets
	if len(targets) != 3 {
		t.Fatalf("expected 3 network targets, got %d", len(targets))
	}

	byPattern := map[string]m.NetworkTargetRule{}
	for _, tgt := range targets {
		byPattern[tgt.Pattern] = tgt
	}

	if ws := byPattern["wss://fstream.binance.com/*"]; len(ws.Limits) != 0 {
		t.Errorf("ws target must not carry limits, got %d", len(ws.Limits))
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
