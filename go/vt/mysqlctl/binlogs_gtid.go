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
	"fmt"
	"sort"
	"strings"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type BackupManifestPath []*BackupManifest

func (p *BackupManifestPath) String() string {
	var sb strings.Builder
	sb.WriteString("BackupManifestPath: [")
	for i, m := range *p {
		if i > 0 {
			sb.WriteString(", ")
		}
		if m.Incremental {
			sb.WriteString("incremental:")
		} else {
			sb.WriteString("full:")
		}
		sb.WriteString(fmt.Sprintf("%v...%v", m.FromPosition, m.Position))
	}
	sb.WriteString("]")
	return sb.String()
}

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
	unionPreviousGTIDs bool,
) (
	binaryLogsToBackup []string,
	incrementalBackupFromGTID string,
	incrementalBackupToGTID string,
	err error,
) {

	var prevGTIDsUnion mysql.GTIDSet
	for i, binlog := range binaryLogs {
		previousGtids, err := pgtids(ctx, binlog)
		if err != nil {
			return nil, "", "", vterrors.Wrapf(err, "cannot get previous gtids for binlog %v", binlog)
		}
		prevPos, err := mysql.ParsePosition(mysql.Mysql56FlavorID, previousGtids)
		if err != nil {
			return nil, "", "", vterrors.Wrapf(err, "cannot decode binlog %s position in incremental backup: %v", binlog, prevPos)
		}
		if prevGTIDsUnion == nil {
			prevGTIDsUnion = prevPos.GTIDSet
		} else {
			prevGTIDsUnion = prevGTIDsUnion.Union(prevPos.GTIDSet)
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
		if unionPreviousGTIDs {
			prevPos.GTIDSet = prevGTIDsUnion
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

// IsValidIncrementalBakcup determines whether the given manifest can be used to extend a backup
// based on baseGTIDSet. The manifest must be able to pick up from baseGTIDSet, and must extend it by at least
// one entry.
func IsValidIncrementalBakcup(baseGTIDSet mysql.GTIDSet, purgedGTIDSet mysql.GTIDSet, manifest *BackupManifest) bool {
	if manifest == nil {
		return false
	}
	if !manifest.Incremental {
		return false
	}
	// We want to validate:
	// manifest.FromPosition <= baseGTID < manifest.Position
	if !baseGTIDSet.Contains(manifest.FromPosition.GTIDSet) {
		// the incremental backup has a gap from the base set.
		return false
	}
	if baseGTIDSet.Contains(manifest.Position.GTIDSet) {
		// the incremental backup adds nothing; it's already contained in the base set
		return false
	}
	if !manifest.Position.GTIDSet.Union(purgedGTIDSet).Contains(baseGTIDSet) {
		// the base set seems to have extra entries?
		return false
	}
	return true
}

// FindPITRPath evaluates the shortest path to recover a restoreToGTIDSet. The past is composed of:
// - a full backup, followed by:
// - zero or more incremental backups
// The path ends with restoreToGTIDSet or goes beyond it. No shorter path will do the same.
// The function returns an error when a path cannot be found.
func FindPITRPath(restoreToGTIDSet mysql.GTIDSet, manifests [](*BackupManifest)) (shortestPath [](*BackupManifest), err error) {
	sortedManifests := make([](*BackupManifest), 0, len(manifests))
	for _, m := range manifests {
		if m != nil {
			sortedManifests = append(sortedManifests, m)
		}
	}
	sort.SliceStable(sortedManifests, func(i, j int) bool {
		return sortedManifests[j].Position.GTIDSet.Union(sortedManifests[i].PurgedPosition.GTIDSet).Contains(sortedManifests[i].Position.GTIDSet)
	})
	mostRelevantFullBackupIndex := -1 // an invalid value
	for i, manifest := range sortedManifests {
		if manifest.Incremental {
			continue
		}
		if restoreToGTIDSet.Contains(manifest.Position.GTIDSet) {
			// This backup is <= desired restore point, therefore it's valid
			mostRelevantFullBackupIndex = i
		}
	}

	if mostRelevantFullBackupIndex < 0 {
		// No full backup prior to desired restore point...
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no full backup found before GTID %v", restoreToGTIDSet)
	}
	// All that interests us starts with mostRelevantFullBackupIndex: that's where the full backup is,
	// and any relevant incremental backups follow that point (because manifests are sorted by backup pos, ascending)
	sortedManifests = sortedManifests[mostRelevantFullBackupIndex:]
	// Of all relevant backups, we take the most recent one.
	fullBackup := sortedManifests[0]
	if restoreToGTIDSet.Equal(fullBackup.Position.GTIDSet) {
		// Perfect match, we don't need to look for incremental backups.
		// We just skip the complexity of the followup section.
		// The result path is a single full backup.
		return append(shortestPath, fullBackup), nil
	}
	purgedGTIDSet := fullBackup.PurgedPosition.GTIDSet

	var validRestorePaths []BackupManifestPath
	// recursive function that searches for all possible paths:
	var findPaths func(baseGTIDSet mysql.GTIDSet, pathManifests []*BackupManifest, remainingManifests []*BackupManifest)
	findPaths = func(baseGTIDSet mysql.GTIDSet, pathManifests []*BackupManifest, remainingManifests []*BackupManifest) {
		// The algorithm was first designed to find all possible paths. But then we recognized that it will be
		// doing excessive work. At this time we choose to end the search once we find the first valid path, even if
		// it's not the most optimal. The next "if" statement is the addition to the algorithm, where we suffice with
		// a single result.
		if len(validRestorePaths) > 0 {
			return
		}
		// remove the above if you wish to explore all paths.
		if baseGTIDSet.Contains(restoreToGTIDSet) {
			// successful end of path. Update list of successful paths
			validRestorePaths = append(validRestorePaths, pathManifests)
			return
		}
		if len(remainingManifests) == 0 {
			// end of the road. No possibilities from here.
			return
		}
		// if the next manifest is eligible to be part of the path, try it out
		if IsValidIncrementalBakcup(baseGTIDSet, purgedGTIDSet, remainingManifests[0]) {
			nextGTIDSet := baseGTIDSet.Union(remainingManifests[0].Position.GTIDSet)
			findPaths(nextGTIDSet, append(pathManifests, remainingManifests[0]), remainingManifests[1:])
		}
		// also, try without the next manifest
		findPaths(baseGTIDSet, pathManifests, remainingManifests[1:])
	}
	// find all paths, entry point
	findPaths(fullBackup.Position.GTIDSet, sortedManifests[0:1], sortedManifests[1:])
	if len(validRestorePaths) == 0 {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no path found that leads to GTID %v", restoreToGTIDSet)
	}
	// Now find a shortest path
	for i := range validRestorePaths {
		path := validRestorePaths[i]
		if shortestPath == nil {
			shortestPath = path
			continue
		}
		if len(path) < len(shortestPath) {
			shortestPath = path
		}
	}
	return shortestPath, nil
}
