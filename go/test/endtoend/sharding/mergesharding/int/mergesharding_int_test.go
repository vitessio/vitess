package v3

import (
	"testing"

	sharding "vitess.io/vitess/go/test/endtoend/sharding/mergesharding"
)

// TestMergeShardingIntShardingKey - tests merge sharding using a INT column
func TestMergeShardingIntShardingKey(t *testing.T) {
	sharding.TestMergesharding(t, false /* useVarbinaryShardingKeyType */)

}
