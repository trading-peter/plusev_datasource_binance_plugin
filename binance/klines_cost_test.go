package binance

import "testing"

// TestFuturesKlinesCost pins the /fapi/v1/klines weight tiers from
// https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data
// ([1,100)=1, [100,500)=2, [500,1000]=5, >1000=10; limit 0 = API default 500).
func TestFuturesKlinesCost(t *testing.T) {
	cases := []struct {
		limit int
		cost  int
	}{
		{limit: 0, cost: 5},     // API default limit 500
		{limit: 1, cost: 1},     // [1,100)
		{limit: 99, cost: 1},    // [1,100)
		{limit: 100, cost: 2},   // [100,500)
		{limit: 499, cost: 2},   // [100,500)
		{limit: 500, cost: 5},   // [500,1000]
		{limit: 1000, cost: 5},  // [500,1000]
		{limit: 1001, cost: 10}, // >1000
		{limit: 1500, cost: 10}, // >1000 (max allowed)
	}
	for _, tc := range cases {
		if got := futuresKlinesCost(tc.limit); got != tc.cost {
			t.Errorf("futuresKlinesCost(%d) = %d, want %d", tc.limit, got, tc.cost)
		}
	}
}
