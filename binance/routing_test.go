package binance

import (
	"encoding/json"
	"strings"
	"testing"

	ex "github.com/plusev-terminal/go-plugin-common/v2/datasrc/exchange"
	"github.com/plusev-terminal/go-plugin-common/v2/plugin"
	rt "github.com/plusev-terminal/go-plugin-common/v2/requester/types"
	tt "github.com/plusev-terminal/go-plugin-common/v2/trading"
)

// recordingDoer captures the request the client built and replies with canned
// JSON, so endpoint routing is observable without touching the network.
type recordingDoer struct {
	url  string
	cost int
	body string
}

func (d *recordingDoer) Send(req *rt.Request, v any) (*rt.Response, error) {
	d.url = req.URL
	d.cost = req.Cost
	if err := json.Unmarshal([]byte(d.body), v); err != nil {
		return nil, err
	}
	return &rt.Response{Status: 200, Body: []byte(d.body)}, nil
}

// klinesJSON is one valid Binance kline row: openTime, open, high, low,
// close, volume, closeTime, …
const klinesJSON = `[[1700000000000,"42000.1","42100.5","41900.2","42050.7","123.45",1700000059999,"1","100","0","0"]]`

// TestIsUSDMFutures pins the routing predicate: linear perps publish as
// "perpetual" and dated contracts as "futures" — BOTH are USDM-futures
// endpoints; spot and everything else are not.
func TestIsUSDMFutures(t *testing.T) {
	for _, at := range []tt.AssetType{tt.AssetTypePerpetual, tt.AssetTypeFutures} {
		if !isUSDMFutures(at) {
			t.Errorf("isUSDMFutures(%q) = false, want true", at)
		}
	}
	for _, at := range []tt.AssetType{tt.AssetTypeSpot, tt.AssetTypeOption, tt.AssetTypeInversePerpetual, ""} {
		if isUSDMFutures(at) {
			t.Errorf("isUSDMFutures(%q) = true, want false", at)
		}
	}
}

// TestGetOHLCVWithAssetTypeRoutesEndpoints guards the klines routing bug:
// perpetual markets must hit the USDM-futures klines endpoint, not spot.
func TestGetOHLCVWithAssetTypeRoutesEndpoints(t *testing.T) {
	cases := []struct {
		assetType tt.AssetType
		wantURL   string
		wantCost  int
	}{
		{tt.AssetTypeSpot, "https://api.binance.com/api/v3/klines", 2},
		{tt.AssetTypePerpetual, "https://fapi.binance.com/fapi/v1/klines", 1},
		{tt.AssetTypeFutures, "https://fapi.binance.com/fapi/v1/klines", 1},
	}

	for _, tc := range cases {
		doer := &recordingDoer{body: klinesJSON}
		c := NewClientWithDoer(doer, "https://api.binance.com")

		records, err := c.GetOHLCVWithAssetType(ex.GetOHLCVParams{
			Market:    tt.Market{Symbol: "BTCUSDT", AssetType: tc.assetType},
			Timeframe: "1m",
			Limit:     50,
		}, tc.assetType)
		if err != nil {
			t.Fatalf("assetType %q: %v", tc.assetType, err)
		}
		if !strings.HasPrefix(doer.url, tc.wantURL+"?") {
			t.Errorf("assetType %q: URL = %q, want prefix %q", tc.assetType, doer.url, tc.wantURL+"?")
		}
		if doer.cost != tc.wantCost {
			t.Errorf("assetType %q: cost = %d, want %d", tc.assetType, doer.cost, tc.wantCost)
		}
		if len(records) != 1 || records[0].Close != "42050.7" {
			t.Errorf("assetType %q: records = %+v", tc.assetType, records)
		}
	}
}

// TestGetAccountInfoRoutesEndpoints guards account routing and its weights
// (spot /api/v3/account = 20, USDM /fapi/v2/account = 5).
func TestGetAccountInfoRoutesEndpoints(t *testing.T) {
	cases := []struct {
		assetType tt.AssetType
		wantURL   string
		wantCost  int
	}{
		{tt.AssetTypeSpot, "https://api.binance.com/api/v3/account", 20},
		{tt.AssetTypePerpetual, "https://fapi.binance.com/fapi/v2/account", 5},
		{tt.AssetTypeFutures, "https://fapi.binance.com/fapi/v2/account", 5},
	}

	for _, tc := range cases {
		doer := &recordingDoer{body: `{"canTrade":true,"balances":[]}`}
		c := NewClientWithDoer(doer, "https://api.binance.com")
		c.SetCredentials(map[string]string{"apiKey": "k", "apiSecret": "s"})

		if _, err := c.GetAccountInfo(tc.assetType); err != nil {
			t.Fatalf("assetType %q: %v", tc.assetType, err)
		}
		if !strings.HasPrefix(doer.url, tc.wantURL+"?") {
			t.Errorf("assetType %q: URL = %q, want prefix %q", tc.assetType, doer.url, tc.wantURL+"?")
		}
		if doer.cost != tc.wantCost {
			t.Errorf("assetType %q: cost = %d, want %d", tc.assetType, doer.cost, tc.wantCost)
		}
	}
}

// TestPrepareStreamRoutesWebSocketByAssetType guards the stream routing bug:
// perpetual and futures kline streams must use fstream, spot stays on the
// spot stream host.
func TestPrepareStreamRoutesWebSocketByAssetType(t *testing.T) {
	cases := []struct {
		assetType string
		wantWS    string
	}{
		{"", "wss://stream.binance.com:9443/ws"},
		{"spot", "wss://stream.binance.com:9443/ws"},
		{"perpetual", "wss://fstream.binance.com/ws"},
		{"futures", "wss://fstream.binance.com/ws"},
	}

	for _, tc := range cases {
		c := NewClientWithDoer(nil, "https://api.binance.com")
		ctx := map[string]any{
			"symbol":    "BTCUSDT",
			"timeframe": "1m",
		}
		if tc.assetType != "" {
			ctx["assetType"] = tc.assetType
		}

		resp, err := c.PrepareStream(plugin.StreamSetupRequest{
			StreamID:   "test",
			StreamType: "ohlcv",
			Parameters: map[string]any{"streamContext": ctx},
		})
		if err != nil || !resp.Success {
			t.Fatalf("assetType %q: %v %+v", tc.assetType, err, resp)
		}
		if resp.WebSocketURL != tc.wantWS {
			t.Errorf("assetType %q: WebSocketURL = %q, want %q", tc.assetType, resp.WebSocketURL, tc.wantWS)
		}
	}
}
