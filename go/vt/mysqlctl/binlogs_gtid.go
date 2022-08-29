/*
Copyright 2022 The Vitess Authors.

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

package mysqlctl

import (
	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// ChooseBinlogsForIncrementalBackup chooses which binary logs need to be backed up in an incremental backup,
// given a list of known binary logs, a function that returns the "Previous GTIDs" per binary log, and a
// position from which to backup (normally the position of last known backup)
// The function returns an error if the request could not be fulfilled: whether backup is not at all
// possible, or is empty.
func ChooseBinlogsForIncrementalBackup(
	ctx context.Context,
	lookFromGTIDSet mysql.GTIDSet,
	binaryLogs []string,
	pgtids func(ctx context.Context, binlog string) (gtids string, err error),
) (
	binaryLogsToBackup []string,
	incrementalBackupFromGTID string,
	incrementalBackupToGTID string,
	err error,
) {

	for i, binlog := range binaryLogs {
		previousGtids, err := pgtids(ctx, binlog)
		if err != nil {
			return nil, "", "", vterrors.Wrapf(err, "cannot get previous gtids for binlog %v", binlog)
		}
		prevPos, err := mysql.ParsePosition(mysql.Mysql56FlavorID, previousGtids)
		if err != nil {
			return nil, "", "", vterrors.Wrapf(err, "cannot decode binlog %s position in incremental backup: %v", binlog, prevPos)
		}
		containedInFromPos := lookFromGTIDSet.Contains(prevPos.GTIDSet)
		// The binary logs are read in-order. They are build one on top of the other: we know
		// the PreviousGTIDs of once binary log fully cover the previous binary log's.
		if containedInFromPos {
			// All previous binary logs are fully contained by backupPos. Carry on
			continue
		}
		// We look for the first binary log whose "PreviousGTIDs" isn't already fully covered
		// by "backupPos" (the position from which we want to create the inreemental backup).
		// That means the *previous* binary log is the first binary log to introduce GTID events on top
		// of "backupPos"
		if i == 0 {
			return nil, "", "", vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "the very first binlog file %v has PreviousGTIDs %s that exceed given incremental backup pos. There are GTID entries that are missing and this backup cannot run", binlog, prevPos)
		}
		if !prevPos.GTIDSet.Contains(lookFromGTIDSet) {
			return nil, "", "", vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "binary log %v with previous GTIDS %s neither contains requested GTID %s nor contains it. Backup cannot take place", binlog, prevPos.GTIDSet, lookFromGTIDSet)
		}
		// We begin with the previous binary log, and we ignore the last binary log, because it's still open and being written to.
		binaryLogsToBackup = binaryLogs[i-1 : len(binaryLogs)-1]
		incrementalBackupFromGTID, err := pgtids(ctx, binaryLogsToBackup[0])
		if err != nil {
			return nil, "", "", vterrors.Wrapf(err, "cannot evaluate incremental backup from pos")
		}
		// The "previous GTIDs" of the binary logs that _follows_ our binary-logs-to-backup indicates
		// the backup's position.
		incrementalBackupToGTID, err := pgtids(ctx, binaryLogs[len(binaryLogs)-1])
		if err != nil {
			return nil, "", "", vterrors.Wrapf(err, "cannot evaluate incremental backup to pos")
		}
		return binaryLogsToBackup, incrementalBackupFromGTID, incrementalBackupToGTID, nil
	}
	return nil, "", "", vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no binary logs to backup (increment is empty)")
}
