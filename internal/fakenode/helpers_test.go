package fakenode

import (
	"testing"

	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
)

// TestClientCfg / NewTestClient are shared test helpers.
func MakeClientCfg(url string) rpc.Config {
	return rpc.Config{Endpoint: url, TimeoutMillis: 1500, MaxRetries: 2, BackoffMillis: 5}
}

func MakeTestClient(t *testing.T, cfg rpc.Config) *rpc.HTTPClient {
	t.Helper()
	return rpc.NewHTTPClient(cfg)
}
