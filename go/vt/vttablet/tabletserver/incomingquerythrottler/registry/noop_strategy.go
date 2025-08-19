package registry

// NoOpStrategy sits in the registry sub-package because registry must produce a safe fallback without importing its parent.
// Moving it up would create a circular dependency (incomingquerythrottler imports registry, and registry would then have to import incomingquerythrottler), which Go prohibits
import (
	"context"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// NoOpStrategy is not intended to be a selectable policy. It exists solely as a hard-wired, last-resort fallback for the registry.
// By instantiating it directly inside CreateStrategy(), we guarantee the system always has some strategy, even when configuration is wrong or plugin registration fails.
// Registering it would make it user-configurable, which opens the door for an accidental ‘no throttling’ setup in production. Hence, it deliberately remains unregistered.
var _ ThrottlingStrategyHandler = (*NoOpStrategy)(nil)

// NoOpStrategy is a fallback strategy that performs no throttling.
type NoOpStrategy struct{}

// Evaluate always returns a decision to not throttle since this is a no-op strategy.
func (s *NoOpStrategy) Evaluate(ctx context.Context, targetTabletType topodatapb.TabletType, sql string, transactionID int64, options *querypb.ExecuteOptions) ThrottleDecision {
	return ThrottleDecision{
		Throttle: false,
		Message:  "NoOpStrategy: no throttling applied",
	}
}

// Start is a no-op for the NoOpStrategy since it requires no initialization.
func (s *NoOpStrategy) Start() {
	// No-op: NoOpStrategy requires no initialization or background processes
}

// Stop is a no-op for the NoOpStrategy since it has no resources to clean up.
func (s *NoOpStrategy) Stop() {
	// No-op: NoOpStrategy has no resources to clean up
}
