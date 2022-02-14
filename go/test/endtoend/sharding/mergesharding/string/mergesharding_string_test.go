package v3

import (
	"testing"

	sharding "vitess.io/vitess/go/test/endtoend/sharding/mergesharding"
)

// TestMergeShardingStringShardingKey - tests merge sharding using a String column
func TestMergeShardingStringShardingKey(t *testing.T) {
	sharding.TestMergesharding(t, true /* useVarbinaryShardingKeyType */)

}
