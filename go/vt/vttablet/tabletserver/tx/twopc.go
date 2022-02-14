package tx

import (
	"time"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// DistributedTx is similar to querypb.TransactionMetadata, but
// is display friendly.
type DistributedTx struct {
	Dtid         string
	State        string
	Created      time.Time
	Participants []querypb.Target
}

// PreparedTx represents a displayable version of a prepared transaction.
type PreparedTx struct {
	Dtid    string
	Queries []string
	Time    time.Time
}
