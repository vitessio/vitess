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

package discovery

import (
	"strings"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TabletErrorAction represents what action to take when a tablet returns an error.
type TabletErrorAction int

const (
	// TabletErrorActionRetry indicates the error is transient and the operation should be retried
	// with the same tablet.
	TabletErrorActionRetry TabletErrorAction = iota
	// TabletErrorActionIgnoreTablet indicates the error is specific to this tablet and the operation
	// should be retried with a different tablet.
	TabletErrorActionIgnoreTablet
	// TabletErrorActionFail indicates the error is not recoverable and the operation should fail.
	TabletErrorActionFail
)

// Determine what action to take when a tablet returns an error.
// Shared by both vstream_manager and vreplication controller workflows.
func ShouldRetryTabletError(err error) TabletErrorAction {
	errCode := vterrors.Code(err)

	// FAILED_PRECONDITION or UNAVAILABLE are transient errors worth retrying.
	if errCode == vtrpcpb.Code_FAILED_PRECONDITION || errCode == vtrpcpb.Code_UNAVAILABLE {
		return TabletErrorActionRetry
	}

	// INVALID_ARGUMENT typically means bad user input, but GTIDSet Mismatch is tablet-specific.
	if errCode == vtrpcpb.Code_INVALID_ARGUMENT {
		if strings.Contains(err.Error(), vterrors.GTIDSetMismatch) {
			return TabletErrorActionIgnoreTablet
		}
		return TabletErrorActionFail
	}

	// INTERNAL errors (like missing journaling participants) require a new stream.
	if errCode == vtrpcpb.Code_INTERNAL {
		return TabletErrorActionFail
	}

	// Handle binary log purging errors by trying a different tablet.
	// This occurs when a tablet doesn't have the requested GTID because the
	// source purged the required binary logs. Another tablet might still have
	// the logs, so we ignore this tablet and retry.
	if errCode == vtrpcpb.Code_UNKNOWN {
		sqlErr := sqlerror.NewSQLErrorFromError(err)
		if sqlError, ok := sqlErr.(*sqlerror.SQLError); ok {
			switch sqlError.Number() {
			case sqlerror.ERMasterFatalReadingBinlog, // 1236
				sqlerror.ERSourceHasPurgedRequiredGtids: // 1789
				return TabletErrorActionIgnoreTablet
			}
		}
	}

	// For ephemeral SQL errors (like MAX_EXECUTION_TIME), retry with the same tablet.
	// We need to parse the error to check if it's a SQL error since the original
	// error may be wrapped in vterrors.
	sqlErr := sqlerror.NewSQLErrorFromError(err)
	if sqlerror.IsEphemeralError(sqlErr) {
		return TabletErrorActionRetry
	}

	// For non-ephemeral errors, don't retry.
	return TabletErrorActionFail
}
