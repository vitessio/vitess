package filters

import (
	"flag"

	"vitess.io/vitess/go/flagutil"
)

// WatchKeyspaces - if provided this specifies which keyspaces should be
// visible to a vtgate. By default the vtgate will allow access to any
// keyspace.
var WatchKeyspaces flagutil.StringListValue

func init() {
	flag.Var(&WatchKeyspaces, "keyspaces_to_watch", "Specifics which keyspaces this vtgate should have access to while routing queries or accessing the vschema")
}
