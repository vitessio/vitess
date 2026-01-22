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
	"strings"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// ParseDestination parses the string representation of a ShardDestination
// of the form keyspace:shard@tablet_type. You can use a / instead of a :.
// It also supports tablet-specific routing with keyspace:shard@tablet_type|tablet-alias
// where tablet-alias is in the format cell-uid (e.g., zone1-0000000100).
// The tablet_type|tablet-alias syntax explicitly specifies both the expected tablet type
// and the specific tablet to route to (e.g., @replica|zone1-0000000100).
func ParseDestination(targetString string, defaultTabletType topodatapb.TabletType) (string, topodatapb.TabletType, key.ShardDestination, *topodatapb.TabletAlias, error) {
	var dest key.ShardDestination
	var keyspace string
	var tabletAlias *topodatapb.TabletAlias
	tabletType := defaultTabletType

	last := strings.LastIndexAny(targetString, "@")
	if last != -1 {
		afterAt := targetString[last+1:]
		if afterAt == "" {
			return "", defaultTabletType, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "empty tablet type after @")
		}
		// Check for explicit tablet_type|tablet_alias syntax (e.g., "replica|zone1-0000000100")
		if pipeIdx := strings.Index(afterAt, "|"); pipeIdx != -1 {
			typeStr := afterAt[:pipeIdx]
			aliasStr := afterAt[pipeIdx+1:]

			// Parse the tablet type
			parsedTabletType, err := ParseTabletType(typeStr)
			if err != nil || parsedTabletType == topodatapb.TabletType_UNKNOWN {
				return "", defaultTabletType, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid tablet type in target: %s", typeStr)
			}
			tabletType = parsedTabletType

			// Parse the tablet alias
			alias, err := ParseTabletAlias(aliasStr)
			if err != nil {
				return "", defaultTabletType, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid tablet alias in target: %s", aliasStr)
			}
			tabletAlias = alias
		} else {
			// No pipe: just a tablet type (backward compatible - allow UNKNOWN)
			tabletType, _ = ParseTabletType(afterAt)
		}
		targetString = targetString[:last]
	}
	last = strings.LastIndexAny(targetString, "/:")
	if last != -1 {
		dest = key.DestinationShard(targetString[last+1:])
		targetString = targetString[:last]
	}
	// Try to parse it as a keyspace id or range
	last = strings.LastIndexAny(targetString, "[")
	if last != -1 {
		rangeEnd := strings.LastIndexAny(targetString, "]")
		if rangeEnd == -1 {
			return keyspace, tabletType, dest, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid key range provided. Couldn't find range end ']'")
		}
		rangeString := targetString[last+1 : rangeEnd]
		if strings.Contains(rangeString, "-") {
			// Parse as range
			keyRange, err := key.ParseShardingSpec(rangeString)
			if err != nil {
				return keyspace, tabletType, dest, nil, err
			}
			if len(keyRange) != 1 {
				return keyspace, tabletType, dest, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "single keyrange expected in %s", rangeString)
			}
			dest = key.DestinationExactKeyRange{KeyRange: keyRange[0]}
		} else {
			// Parse as keyspace id
			destBytes, err := hex.DecodeString(rangeString)
			if err != nil {
				return keyspace, tabletType, dest, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "expected valid hex in keyspace id %s", rangeString)
			}
			dest = key.DestinationKeyspaceID(destBytes)
		}
		targetString = targetString[:last]
	}
	keyspace = targetString
	if tabletAlias != nil && dest == nil {
		return "", defaultTabletType, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "tablet alias must be used with a shard")
	}
	return keyspace, tabletType, dest, tabletAlias, nil
}
