package vtgate

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/callerid"
)

// appends the immediateCallerID to a SQL query as a trailing comment.
// the comment persists through MySQL to the general logs.

func addCallerIDUserToQuery(ctx context.Context, sql string) string {
	return fmt.Sprintf("%s/* user:%s */", sql, callerid.ImmediateCallerIDFromContext(ctx).GetUsername())
}
