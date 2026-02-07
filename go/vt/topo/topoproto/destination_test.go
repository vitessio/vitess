/*
Copyright 2019 The Vitess Authors.

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

package topoproto

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/key"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestParseDestination(t *testing.T) {
	tenHexBytes, _ := hex.DecodeString("10")
	twentyHexBytes, _ := hex.DecodeString("20")

	testcases := []struct {
		targetString string
		dest         key.ShardDestination
		keyspace     string
		tabletType   topodatapb.TabletType
		tabletAlias  *topodatapb.TabletAlias
	}{{
		targetString: "ks[10-20]@primary",
		keyspace:     "ks",
		tabletType:   topodatapb.TabletType_PRIMARY,
		dest:         key.DestinationExactKeyRange{KeyRange: &topodatapb.KeyRange{Start: tenHexBytes, End: twentyHexBytes}},
	}, {
		targetString: "ks[-]@primary",
		keyspace:     "ks",
		tabletType:   topodatapb.TabletType_PRIMARY,
		dest:         key.DestinationExactKeyRange{KeyRange: &topodatapb.KeyRange{}},
	}, {
		targetString: "ks[deadbeef]@primary",
		keyspace:     "ks",
		tabletType:   topodatapb.TabletType_PRIMARY,
		dest:         key.DestinationKeyspaceID([]byte("\xde\xad\xbe\xef")),
	}, {
		targetString: "ks[10-]@primary",
		keyspace:     "ks",
		tabletType:   topodatapb.TabletType_PRIMARY,
		dest:         key.DestinationExactKeyRange{KeyRange: &topodatapb.KeyRange{Start: tenHexBytes}},
	}, {
		targetString: "ks[-20]@primary",
		keyspace:     "ks",
		tabletType:   topodatapb.TabletType_PRIMARY,
		dest:         key.DestinationExactKeyRange{KeyRange: &topodatapb.KeyRange{End: twentyHexBytes}},
	}, {
		targetString: "ks:-80@primary",
		keyspace:     "ks",
		tabletType:   topodatapb.TabletType_PRIMARY,
		dest:         key.DestinationShard("-80"),
	}, {
		targetString: ":-80@primary",
		keyspace:     "",
		tabletType:   topodatapb.TabletType_PRIMARY,
		dest:         key.DestinationShard("-80"),
	}, {
		targetString: "@primary",
		keyspace:     "",
		tabletType:   topodatapb.TabletType_PRIMARY,
	}, {
		targetString: "@replica",
		keyspace:     "",
		tabletType:   topodatapb.TabletType_REPLICA,
	}, {
		targetString: "ks",
		keyspace:     "ks",
		tabletType:   topodatapb.TabletType_PRIMARY,
	}, {
		targetString: "ks/-80",
		keyspace:     "ks",
		dest:         key.DestinationShard("-80"),
		tabletType:   topodatapb.TabletType_PRIMARY,
	}, {
		targetString: "ks:-80@primary|zone1-0000000100",
		keyspace:     "ks",
		dest:         key.DestinationShard("-80"),
		tabletType:   topodatapb.TabletType_PRIMARY,
		tabletAlias:  &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
	}, {
		targetString: "ks:-80@replica|zone1-0000000101",
		keyspace:     "ks",
		dest:         key.DestinationShard("-80"),
		tabletType:   topodatapb.TabletType_REPLICA,
		tabletAlias:  &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
	}, {
		targetString: "ks:80-@rdonly|zone2-0000000200",
		keyspace:     "ks",
		dest:         key.DestinationShard("80-"),
		tabletType:   topodatapb.TabletType_RDONLY,
		tabletAlias:  &topodatapb.TabletAlias{Cell: "zone2", Uid: 200},
	}}

	for _, tcase := range testcases {
		if targetKeyspace, targetTabletType, targetDest, targetAlias, _ := ParseDestination(tcase.targetString, topodatapb.TabletType_PRIMARY); !reflect.DeepEqual(targetDest, tcase.dest) || targetKeyspace != tcase.keyspace || targetTabletType != tcase.tabletType || !reflect.DeepEqual(targetAlias, tcase.tabletAlias) {
			t.Errorf("ParseDestination(%s) - got: (%v, %v, %v, %v), want (%v, %v, %v, %v)",
				tcase.targetString,
				targetKeyspace,
				targetTabletType,
				targetDest,
				targetAlias,
				tcase.keyspace,
				tcase.tabletType,
				tcase.dest,
				tcase.tabletAlias,
			)
		}
	}

	_, _, _, _, err := ParseDestination("ks[20-40-60]", topodatapb.TabletType_PRIMARY)
	require.EqualError(t, err, "single keyrange expected in 20-40-60")

	_, _, _, _, err = ParseDestination("ks[--60]", topodatapb.TabletType_PRIMARY)
	require.EqualError(t, err, "malformed spec: MinKey/MaxKey cannot be in the middle of the spec: \"--60\"")

	_, _, _, _, err = ParseDestination("ks[qrnqorrs]@primary", topodatapb.TabletType_PRIMARY)
	require.EqualError(t, err, "expected valid hex in keyspace id qrnqorrs")

	_, _, _, _, err = ParseDestination("ks@primary|zone1-0000000100", topodatapb.TabletType_PRIMARY)
	require.EqualError(t, err, "tablet alias must be used with a shard")

	// Test invalid tablet type in pipe syntax
	_, _, _, _, err = ParseDestination("ks:-80@invalid|zone1-0000000100", topodatapb.TabletType_PRIMARY)
	require.EqualError(t, err, "invalid tablet type in target: invalid")

	// Test invalid tablet alias in pipe syntax
	_, _, _, _, err = ParseDestination("ks:-80@primary|invalid-alias", topodatapb.TabletType_PRIMARY)
	require.EqualError(t, err, "invalid tablet alias in target: invalid-alias")

	// Test unknown tablet type in pipe syntax
	_, _, _, _, err = ParseDestination("ks:-80@unknown|zone1-0000000100", topodatapb.TabletType_PRIMARY)
	require.EqualError(t, err, "invalid tablet type in target: unknown")

	// Test empty tablet type in pipe syntax
	_, _, _, _, err = ParseDestination("ks:-80@|zone1-0000000100", topodatapb.TabletType_PRIMARY)
	require.EqualError(t, err, "invalid tablet type in target: ")

	// Test empty tablet alias in pipe syntax
	_, _, _, _, err = ParseDestination("ks:-80@primary|", topodatapb.TabletType_PRIMARY)
	require.EqualError(t, err, "invalid tablet alias in target: ")

	// Test empty string after @
	_, _, _, _, err = ParseDestination("ks@", topodatapb.TabletType_PRIMARY)
	require.EqualError(t, err, "empty tablet type after @")
}
