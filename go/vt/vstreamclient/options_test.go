/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vstreamclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithStateTable_RequiresTableName(t *testing.T) {
	v := &VStreamClient{shardsByKeyspace: map[string][]string{"ks": {"0"}}}

	err := WithStateTable("ks", "")(v)
	require.ErrorContains(t, err, "state table name is required")
}

func TestWithStateTable_RejectsUnknownKeyspace(t *testing.T) {
	v := &VStreamClient{shardsByKeyspace: map[string][]string{"ks": {"0"}}}

	err := WithStateTable("missing", "state")(v)
	require.ErrorContains(t, err, "keyspace missing not found")
}

func TestWithStateTable_RejectsShardedKeyspace(t *testing.T) {
	v := &VStreamClient{shardsByKeyspace: map[string][]string{"sharded": {"-80", "80-"}}}

	err := WithStateTable("sharded", "state")(v)
	require.ErrorContains(t, err, "only unsharded keyspaces are supported")
}

func TestWithStateTable_EscapesIdentifiers(t *testing.T) {
	v := &VStreamClient{shardsByKeyspace: map[string][]string{"ks": {"0"}}}

	err := WithStateTable("ks", "state")(v)
	require.NoError(t, err)

	// the identifiers are stored pre-escaped, since every state query interpolates them
	assert.Equal(t, "`ks`", v.cfg.vgtidStateKeyspace)
	assert.Equal(t, "`state`", v.cfg.vgtidStateTable)
}
