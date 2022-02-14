package v3

import (
	"testing"

	sharding "vitess.io/vitess/go/test/endtoend/sharding/resharding"
)

// TestV3ReSharding - main tests resharding using a INT column
func TestV3ReSharding(t *testing.T) {
	sharding.TestResharding(t, false)

}
