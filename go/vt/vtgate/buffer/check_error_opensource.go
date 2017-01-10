package buffer

import (
	"strings"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/vterrors"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// This function is in a separate file to make it easier to swap out an
// open-source implementation with any internal Google-only implementation.

func causedByFailover(err error) bool {
	log.V(2).Infof("Checking error (type: %T) if it is caused by a failover. err: %v", err, err)
	if vtErr, ok := err.(vterrors.VtError); ok {
		if vtErr.VtErrorCode() == vtrpcpb.ErrorCode_QUERY_NOT_SERVED {
			if strings.Contains(err.Error(), "retry: operation not allowed in state NOT_SERVING") ||
				strings.Contains(err.Error(), "retry: operation not allowed in state SHUTTING_DOWN") ||
				strings.Contains(err.Error(), "retry: The MariaDB server is running with the --read-only option so it cannot execute this statement (errno 1290) (sqlstate HY000)") ||
				strings.Contains(err.Error(), "retry: The MySQL server is running with the --read-only option so it cannot execute this statement (errno 1290) (sqlstate HY000)") ||
				// Match 1290 if -queryserver-config-terse-errors explicitly hid the error message (which it does to avoid logging the original query including any PII).
				strings.Contains(err.Error(), "retry: (errno 1290) (sqlstate HY000) during query:") {
				return true
			}
		}
	}
	return false
}
