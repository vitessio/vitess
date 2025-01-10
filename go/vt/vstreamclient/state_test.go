package vstreamclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewVGtid_DeduplicatesKeyspaces(t *testing.T) {
	tables := map[string]*TableConfig{
		"t1": {Keyspace: "ks1", Table: "table_a"},
		"t2": {Keyspace: "ks1", Table: "table_b"},
		"t3": {Keyspace: "ks2", Table: "table_c"},
	}
	shardsByKeyspace := map[string][]string{
		"ks1": {"-80", "80-"},
		"ks2": {"0"},
	}

	vgtid, err := newVGtid(tables, shardsByKeyspace)
	assert.NoError(t, err)
	if !assert.NotNil(t, vgtid) {
		return
	}

	counts := make(map[string]int)
	for _, shardGtid := range vgtid.ShardGtids {
		counts[shardGtid.Keyspace]++
	}

	assert.Equal(t, 2, counts["ks1"])
	assert.Equal(t, 1, counts["ks2"])
	assert.Len(t, vgtid.ShardGtids, 3)
}

func TestNewVGtid_MissingKeyspaceErrors(t *testing.T) {
	tables := map[string]*TableConfig{
		"t1": {Keyspace: "missing", Table: "table_a"},
	}

	_, err := newVGtid(tables, map[string][]string{"ks1": {"0"}})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "keyspace missing not found")
}
