package incomingquerythrottler

import "context"

type ConfigLoader interface {
	// Load returns the latest throttler config (may come from file, topo, etc.)
	Load(ctx context.Context) (Config, error)
}
