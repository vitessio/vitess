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
	"time"

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
	backupFromGTIDSet mysql.GTIDSet,
	purgedGTIDSet mysql.GTIDSet,
	binaryLogs []string,
	pgtids func(ctx context.Context, binlog string) (gtids string, err error),
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
		previousGTIDsPos, err := mysql.ParsePosition(mysql.Mysql56FlavorID, previousGtids)
		if err != nil {
			return nil, "", "", vterrors.Wrapf(err, "cannot decode binlog %s position in incremental backup: %v", binlog, previousGTIDsPos)
		}
		if prevGTIDsUnion == nil {
			prevGTIDsUnion = previousGTIDsPos.GTIDSet
		} else {
			prevGTIDsUnion = prevGTIDsUnion.Union(previousGTIDsPos.GTIDSet)
		}

		// The binary logs are read in-order. They expand. For example, we know
		// Previous-GTIDs of binlog file 0000018 contain those of binlog file 0000017.
		// We look for the first binary log whose Previous-GTIDs isn't already fully covered
		// by "backupPos" (the position from which we want to create the incremental backup).
		// That means the *previous* binary log is the first binary log to introduce GTID events on top
		// of "backupPos"
		if backupFromGTIDSet.Contains(previousGTIDsPos.GTIDSet) {
			// Previous-GTIDs is contained by backupPos. So definitely all binlogs _prior_ to
			// this binlog are not necessary. We still don't know about _this_ binlog. We can't tell yet if
			// _this_ binlog introduces new GTID entries not covered by the last backup pos. But we will only
			// know this when we look into the _next_ binlog file's Previous-GTIDs.
			continue
		}
		if i == 0 {
			return nil, "", "", vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "Required entries have been purged. Oldest binary log %v expects entries not found in backup pos. Expected pos=%v", binlog, previousGTIDsPos)
		}
		if !prevGTIDsUnion.Union(purgedGTIDSet).Contains(backupFromGTIDSet) {
			return nil, "", "", vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION,
				"Mismatching GTID entries. Requested backup pos has entries not found in the binary logs, and binary logs have entries not found in the requested backup pos. Neither fully contains the other. Requested pos=%v, binlog pos=%v",
				backupFromGTIDSet, previousGTIDsPos.GTIDSet)
		}
		// We begin with the previous binary log, and we ignore the last binary log, because it's still open and being written to.
		binaryLogsToBackup = binaryLogs[i-1 : len(binaryLogs)-1]
		incrementalBackupFromGTID, err := pgtids(ctx, binaryLogsToBackup[0])
		if err != nil {
			return nil, "", "", vterrors.Wrapf(err, "cannot evaluate incremental backup from pos")
		}
		if incrementalBackupFromGTID == "" {
			// This can happen on the very first binary log file. It happens in two scenarios:
			// 1. This is the first binlog ever in the history of the mysql server; the GTID is truly empty
			// 2. A full backup was taken and restored, with all binlog scrapped.
			// We take for granted that the first binary log file covers the
			// requested "from GTID"
			incrementalBackupFromGTID = backupFromGTIDSet.String()
		}

		// The Previous-GTIDs of the binary logs that _follows_ our binary-logs-to-backup indicates
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

// FindPITRToTimePath evaluates the shortest path to recover a restoreToGTIDSet. The past is composed of:
// - a full backup, followed by:
// - zero or more incremental backups
// The path ends with restoreToGTIDSet or goes beyond it. No shorter path will do the same.
// The function returns an error when a path cannot be found.
func FindPITRToTimePath(restoreToTime time.Time, manifests [](*BackupManifest)) (shortestPath [](*BackupManifest), err error) {
	restoreToTimeStr := FormatRFC3339(restoreToTime)
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
		startTime, err := ParseRFC3339(manifest.BackupTime)
		if err != nil {
			return nil, vterrors.Wrapf(err, "parsing manifest BackupTime %s", manifest.BackupTime)
		}
		finishedTime, err := ParseRFC3339(manifest.FinishedTime)
		if err != nil {
			return nil, vterrors.Wrapf(err, "parsing manifest FinishedTime %s", manifest.FinishedTime)
		}
		var compareWithTime time.Time
		switch manifest.BackupMethod {
		case xtrabackupEngineName:
			// Xtrabackup backups are true to the time they complete (the snapshot is taken at the very end).
			// Therefore the finish time best represents the backup time.
			compareWithTime = finishedTime
		case builtinBackupEngineName:
			// Builtin takes down the MySQL server. Hence the _start time_ represents the backup time best
			compareWithTime = startTime
		default:
			compareWithTime = startTime
		}
		if restoreToTime.Before(compareWithTime) {
			// We want a bfull backup whose time is _before_ restore-to-time, and we will top it with
			// inremental restore via binlogs.
			continue
		}
		mostRelevantFullBackupIndex = i
	}

	if mostRelevantFullBackupIndex < 0 {
		// No full backup prior to desired restore point...
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no full backup found before timestmap %v", restoreToTimeStr)
	}
	// All that interests us starts with mostRelevantFullBackupIndex: that's where the full backup is,
	// and any relevant incremental backups follow that point (because manifests are sorted by backup pos, ascending)
	sortedManifests = sortedManifests[mostRelevantFullBackupIndex:]
	// Of all relevant backups, we take the most recent one.
	fullBackup := sortedManifests[0]
	purgedGTIDSet := fullBackup.PurgedPosition.GTIDSet

	timeIsInRange := func(t, from, to time.Time) bool {
		// integrity:
		if to.Before(from) {
			return false // bad input
		}
		if t.Before(from) {
			return false
		}
		if t.After(to) {
			return false
		}
		return true
	}

	var validRestorePaths []BackupManifestPath
	// recursive function that searches for all possible paths:
	var findPaths func(baseGTIDSet mysql.GTIDSet, pathManifests []*BackupManifest, remainingManifests []*BackupManifest) error
	findPaths = func(baseGTIDSet mysql.GTIDSet, pathManifests []*BackupManifest, remainingManifests []*BackupManifest) error {
		// The algorithm was first designed to find all possible paths. But then we recognized that it will be
		// doing excessive work. At this time we choose to end the search once we find the first valid path, even if
		// it's not the most optimal. The next "if" statement is the addition to the algorithm, where we suffice with
		// a single result.
		if len(validRestorePaths) > 0 {
			return nil
		}
		// remove the above if you wish to explore all paths.
		lastManifest := pathManifests[len(pathManifests)-1]
		if lastManifest.Incremental {
			lastManifestIncrementalDetails := lastManifest.IncrementalDetails

			firstTimestamp, err := ParseRFC3339(lastManifestIncrementalDetails.FirstTimestamp)
			if err != nil {
				return err
			}
			if restoreToTime.Before(firstTimestamp) {
				// the restore-to-time falls between previous manifest's timestamp (whether previous manifest is a
				// full backup or incremental backup is not important), and this manifest's first-timestamp.
				// This means the previous manifest is the end of a valid restore path. We couldn't know it back then.
				validRestorePaths = append(validRestorePaths, pathManifests[0:len(pathManifests)-1])
				return nil
			}
			lastTimestamp, err := ParseRFC3339(lastManifestIncrementalDetails.LastTimestamp)
			if err != nil {
				return err
			}
			if timeIsInRange(restoreToTime, firstTimestamp, lastTimestamp) {
				// successful end of path. Update list of successful paths
				validRestorePaths = append(validRestorePaths, pathManifests)
				return nil
			}
		}
		if len(remainingManifests) == 0 {
			// end of the road. No possibilities from here.
			return nil
		}
		// if the next manifest is eligible to be part of the path, try it out
		if IsValidIncrementalBakcup(baseGTIDSet, purgedGTIDSet, remainingManifests[0]) {
			nextGTIDSet := baseGTIDSet.Union(remainingManifests[0].Position.GTIDSet)
			findPaths(nextGTIDSet, append(pathManifests, remainingManifests[0]), remainingManifests[1:])
		}
		// also, try without the next manifest
		findPaths(baseGTIDSet, pathManifests, remainingManifests[1:])
		return nil
	}
	// find all paths, entry point
	if err := findPaths(fullBackup.Position.GTIDSet, sortedManifests[0:1], sortedManifests[1:]); err != nil {
		return nil, err
	}
	if len(validRestorePaths) == 0 {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no path found that leads to timestamp %v", restoreToTimeStr)
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
