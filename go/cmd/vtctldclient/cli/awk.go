package cli

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// MarshalMapAWK returns a string representation of a string->string map in an
// AWK-friendly format.
func MarshalMapAWK(m map[string]string) string {
	pairs := make([]string, len(m))
	i := 0

	for k, v := range m {
		pairs[i] = fmt.Sprintf("%v: %q", k, v)

		i++
	}

	sort.Strings(pairs)

	return "[" + strings.Join(pairs, " ") + "]"
}

// MarshalTabletAWK marshals a tablet into an AWK-friendly line.
func MarshalTabletAWK(t *topodatapb.Tablet) string {
	ti := topo.TabletInfo{
		Tablet: t,
	}

	keyspace := t.Keyspace
	if keyspace == "" {
		keyspace = "<null>"
	}

	shard := t.Shard
	if shard == "" {
		shard = "<null>"
	}

	mtst := "<null>"
	// special case for old primary that hasn't been updated in the topo
	// yet.
	if t.PrimaryTermStartTime != nil && t.PrimaryTermStartTime.Seconds > 0 {
		mtst = logutil.ProtoToTime(t.PrimaryTermStartTime).Format(time.RFC3339)
	}

	return fmt.Sprintf("%v %v %v %v %v %v %v %v", topoproto.TabletAliasString(t.Alias), keyspace, shard, topoproto.TabletTypeLString(t.Type), ti.Addr(), ti.MysqlAddr(), MarshalMapAWK(t.Tags), mtst)
}
