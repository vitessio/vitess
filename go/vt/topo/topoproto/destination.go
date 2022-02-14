package topoproto

import (
	"encoding/hex"
	"strings"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// ParseDestination parses the string representation of a Destination
// of the form keyspace:shard@tablet_type. You can use a / instead of a :.
func ParseDestination(targetString string, defaultTabletType topodatapb.TabletType) (string, topodatapb.TabletType, key.Destination, error) {
	var dest key.Destination
	var keyspace string
	tabletType := defaultTabletType

	last := strings.LastIndexAny(targetString, "@")
	if last != -1 {
		// No need to check the error. UNKNOWN will be returned on
		// error and it will fail downstream.
		tabletType, _ = ParseTabletType(targetString[last+1:])
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
			return keyspace, tabletType, dest, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid key range provided. Couldn't find range end ']'")
		}
		rangeString := targetString[last+1 : rangeEnd]
		if strings.Contains(rangeString, "-") {
			// Parse as range
			keyRange, err := key.ParseShardingSpec(rangeString)
			if err != nil {
				return keyspace, tabletType, dest, err
			}
			if len(keyRange) != 1 {
				return keyspace, tabletType, dest, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "single keyrange expected in %s", rangeString)
			}
			dest = key.DestinationExactKeyRange{KeyRange: keyRange[0]}
		} else {
			// Parse as keyspace id
			destBytes, err := hex.DecodeString(rangeString)
			if err != nil {
				return keyspace, tabletType, dest, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "expected valid hex in keyspace id %s", rangeString)
			}
			dest = key.DestinationKeyspaceID(destBytes)
		}
		targetString = targetString[:last]
	}
	keyspace = targetString
	return keyspace, tabletType, dest, nil
}
