package v3

import (
	"testing"

	sharding "vitess.io/vitess/go/test/endtoend/sharding/resharding"
)

// TestReshardingString - using a VARBINARY column for resharding.
func TestReshardingString(t *testing.T) {
	sharding.TestResharding(t, true)

}
