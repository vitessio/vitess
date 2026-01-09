/*
Copyright 2019 The Vitess Authors.

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
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/ioutil"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/os2"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	stats "vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	mysqlctlpb "vitess.io/vitess/go/vt/proto/mysqlctl"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	builtinBackupEngineName = "builtin"
	AutoIncrementalFromPos  = "auto"
	dataDictionaryFile      = "mysql.ibd"

	// How many times we will retry file operations. Note that a file operation that
	// returns a vtrpc.Code_FAILED_PRECONDITION error is considered fatal and we will
	// not retry.
	maxRetriesPerFile   = 1
	maxFileCloseRetries = 20 // At this point we should consider it permanent
)

var (
	// BuiltinBackupMysqldTimeout is how long ExecuteBackup should wait for response from mysqld.Shutdown.
	// It can later be extended for other calls to mysqld during backup functions.
	// Exported for testing.
	BuiltinBackupMysqldTimeout = 10 * time.Minute

	builtinBackupProgress = 5 * time.Second

	// Controls the size of the IO buffer used when reading files during backups.
	builtinBackupFileReadBufferSize uint

	// Controls the size of the IO buffer used when writing files during restores.
	builtinBackupFileWriteBufferSize uint = 2 * 1024 * 1024 /* 2 MiB */

	// Controls the size of the IO buffer used when writing to backupstorage
	// engines during backups.  The backupstorage may be a physical file,
	// network, or something else.
	builtinBackupStorageWriteBufferSize = 2 * 1024 * 1024 /* 2 MiB */

	// The directory where incremental restore files, namely binlog files, are extracted to.
	// In k8s environments, this should be set to a directory that is shared between the vttablet and mysqld pods.
	// The path should exist.
	// When empty, the default OS temp dir is assumed.
	builtinIncrementalRestorePath = ""
)

// BuiltinBackupEngine encapsulates the logic of the builtin engine
// it implements the BackupEngine interface and contains all the logic
// required to implement a backup/restore by copying files from and to
// the correct location / storage bucket
type BuiltinBackupEngine struct{}

// builtinBackupManifest represents the backup. It lists all the files, the
// Position that the backup was taken at, the compression engine used, etc.
type builtinBackupManifest struct {
	// BackupManifest is an anonymous embedding of the base manifest struct.
	BackupManifest

	// CompressionEngine stores which compression engine was originally provided
	// to compress the files. Please note that if user has provided externalCompressorCmd
	// then it will contain value 'external'. This field is used during restore routine to
	// get a hint about what kind of compression was used.
	CompressionEngine string `json:",omitempty"`

	// FileEntries contains all the files in the backup
	FileEntries []FileEntry

	// SkipCompress is true if the backup files were NOT run through gzip.
	// The field is expressed as a negative because it will come through as
	// false for backups that were created before the field existed, and those
	// backups all had compression enabled.
	SkipCompress bool

	// When CompressionEngine is "external", ExternalDecompressor may be
	// consulted for the external decompressor command.
	//
	// When taking a backup with --compression-engine=external,
	// ExternalDecompressor will be set to the value of
	// --manifest-external-decompressor, if set, or else left as an empty
	// string.
	//
	// When restoring from a backup with CompressionEngine "external",
	// --external-decompressor will be consulted first and, if that is not set,
	// ExternalDecompressor will be used. If neither are set, the restore will
	// abort.
	ExternalDecompressor string
}

// FileEntry is one file to backup
type FileEntry struct {
	// Base is one of:
	// - backupInnodbDataHomeDir for files that go into Mycnf.InnodbDataHomeDir
	// - backupInnodbLogGroupHomeDir for files that go into Mycnf.InnodbLogGroupHomeDir
	// - binLogDir for files that go in the binlog dir (base path of Mycnf.BinLogPath)
	// - backupData for files that go into Mycnf.DataDir
	Base string

	// Name is the file name, relative to Base
	Name string

	// Hash is the hash of the final data (transformed and
	// compressed if specified) stored in the BackupStorage.
	Hash string

	// ParentPath is an optional prefix to the Base path. If empty, it is ignored. Useful
	// for writing files in a temporary directory
	ParentPath string

	// RetryCount specifies how many times we retried restoring/backing up this FileEntry.
	// If we fail to restore/backup this FileEntry, we will retry up to maxRetriesPerFile times.
	// Every time the builtin backup engine retries this file, we increment this field by 1.
	// We don't care about adding this information to the MANIFEST and also to not cause any compatibility issue
	// we are adding the - json tag to let Go know it can ignore the field.
	RetryCount int `json:"-"`
}

func init() {
	for _, cmd := range []string{"vtbackup", "vtcombo", "vttablet", "vttestserver", "vtctld", "vtctldclient"} {
		servenv.OnParseFor(cmd, registerBuiltinBackupEngineFlags)
	}
}

func registerBuiltinBackupEngineFlags(fs *pflag.FlagSet) {
	utils.SetFlagDurationVar(fs, &BuiltinBackupMysqldTimeout, "builtinbackup-mysqld-timeout", BuiltinBackupMysqldTimeout, "how long to wait for mysqld to shutdown at the start of the backup.")
	utils.SetFlagDurationVar(fs, &builtinBackupProgress, "builtinbackup-progress", builtinBackupProgress, "how often to send progress updates when backing up large files.")
	fs.UintVar(&builtinBackupFileReadBufferSize, "builtinbackup-file-read-buffer-size", builtinBackupFileReadBufferSize, "read files using an IO buffer of this many bytes. Golang defaults are used when set to 0.")
	fs.UintVar(&builtinBackupFileWriteBufferSize, "builtinbackup-file-write-buffer-size", builtinBackupFileWriteBufferSize, "write files using an IO buffer of this many bytes. Golang defaults are used when set to 0.")
	fs.StringVar(&builtinIncrementalRestorePath, "builtinbackup-incremental-restore-path", builtinIncrementalRestorePath, "the directory where incremental restore files, namely binlog files, are extracted to. In k8s environments, this should be set to a directory that is shared between the vttablet and mysqld pods. The path should exist. When empty, the default OS temp dir is assumed.")
}

// fullPath returns the full path of the entry, based on its type
func (fe *FileEntry) fullPath(cnf *Mycnf) (string, error) {
	// find the root to use
	var root string
	switch fe.Base {
	case backupInnodbDataHomeDir:
		root = cnf.InnodbDataHomeDir
	case backupInnodbLogGroupHomeDir:
		root = cnf.InnodbLogGroupHomeDir
	case backupData:
		root = cnf.DataDir
	case backupBinlogDir:
		root = filepath.Dir(cnf.BinLogPath)
	default:
		return "", vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "unknown base: %v", fe.Base)
	}

	return path.Join(fe.ParentPath, root, fe.Name), nil
}

// open attempts to open the file
func (fe *FileEntry) open(cnf *Mycnf, readOnly bool) (*os.File, error) {
	name, err := fe.fullPath(cnf)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot evaluate full name for %v", fe.Name)
	}
	var fd *os.File
	if readOnly {
		if fd, err = openForSequential(name); err != nil {
			return nil, vterrors.Wrapf(err, "cannot open source file %v", name)
		}
	} else {
		dir := path.Dir(name)
		if err := os2.MkdirAll(dir); err != nil {
			return nil, vterrors.Wrapf(err, "cannot create destination directory %v", dir)
		}
		if fd, err = os2.Create(name); err != nil {
			return nil, vterrors.Wrapf(err, "cannot create destination file %v", name)
		}
	}
	return fd, nil
}

// ExecuteBackup runs a backup based on given params. This could be a full or incremental backup.
// The function returns a BackupResult that indicates the usability of the backup, and an overall error.
func (be *BuiltinBackupEngine) ExecuteBackup(ctx context.Context, params BackupParams, bh backupstorage.BackupHandle) (BackupResult, error) {
	params.Logger.Infof("Executing Backup at %v for keyspace/shard %v/%v on tablet %v, concurrency: %v, compress: %v, incrementalFromPos: %v",
		params.BackupTime, params.Keyspace, params.Shard, params.TabletAlias, params.Concurrency, backupStorageCompress, params.IncrementalFromPos)

	if isIncrementalBackup(params) {
		return be.executeIncrementalBackup(ctx, params, bh)
	}
	return be.executeFullBackup(ctx, params, bh)
}

// getIncrementalFromPosGTIDSet turns the given string into a valid Mysql56GTIDSet
func getIncrementalFromPosGTIDSet(incrementalFromPos string) (replication.Mysql56GTIDSet, error) {
	_, gtidSet, err := replication.DecodePositionMySQL56(incrementalFromPos)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot decode position in incremental backup: %v", incrementalFromPos)
	}
	return gtidSet, nil
}

// executeIncrementalBackup runs an incremental backup, based on given 'incremental_from_pos', which can be:
// - A valid position
// - "auto", indicating the incremental backup should begin with last successful backup end position.
// The function returns a BackupResult that indicates the usability of the backup, and an overall error.
func (be *BuiltinBackupEngine) executeIncrementalBackup(ctx context.Context, params BackupParams, bh backupstorage.BackupHandle) (BackupResult, error) {
	// Collect MySQL status:
	// UUID
	serverUUID, err := params.Mysqld.GetServerUUID(ctx)
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "can't get server uuid")
	}
	mysqlVersion, err := params.Mysqld.GetVersionString(ctx)
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "can't get MySQL version")
	}

	// We now need to figure out the GTIDSet from which we want to take the incremental backup. The user may have
	// specified a position, or they may have specified "auto", or they may have specified a backup name, in which
	// case we need to find the position of that backup.
	var fromBackupName string
	if params.IncrementalFromPos == AutoIncrementalFromPos {
		// User has supplied "auto".
		params.Logger.Infof("auto evaluating incremental_from_pos")
		backupName, pos, err := findLatestSuccessfulBackupPosition(ctx, params, bh.Name())
		if err != nil {
			return BackupUnusable, err
		}
		fromBackupName = backupName
		params.IncrementalFromPos = replication.EncodePosition(pos)
		params.Logger.Infof("auto evaluated incremental_from_pos: %s", params.IncrementalFromPos)
	}

	if _, _, err := replication.DecodePositionMySQL56(params.IncrementalFromPos); err != nil {
		// This does not seem to be a valid position. Maybe it's a backup name?
		backupName := params.IncrementalFromPos
		pos, err := findBackupPosition(ctx, params, backupName)
		if err != nil {
			return BackupUnusable, err
		}
		fromBackupName = backupName
		params.IncrementalFromPos = replication.EncodePosition(pos)
		params.Logger.Infof("evaluated incremental_from_pos using backup name %q: %s", backupName, params.IncrementalFromPos)
	}

	// params.IncrementalFromPos is a string. We want to turn that into a MySQL GTID
	backupFromGTIDSet, err := getIncrementalFromPosGTIDSet(params.IncrementalFromPos)
	if err != nil {
		return BackupUnusable, err
	}
	// OK, we now have the formal MySQL GTID from which we want to take the incremental backup.

	// binlogs may not contain information about purged GTIDs. e.g. some binlog.000003 may have
	// previous GTIDs like 00021324-1111-1111-1111-111111111111:30-60, ie 1-29 range is missing. This can happen
	// when a server is restored from backup and set with gtid_purged != "".
	// This is fine!
	// Shortly we will compare a binlog's "Previous GTIDs" with the backup's position. For the purpose of comparison, we
	// ignore the purged GTIDs:

	if err := params.Mysqld.FlushBinaryLogs(ctx); err != nil {
		return BackupUnusable, vterrors.Wrapf(err, "cannot flush binary logs in incremental backup")
	}
	binaryLogs, err := params.Mysqld.GetBinaryLogs(ctx)
	if err != nil {
		return BackupUnusable, vterrors.Wrapf(err, "cannot get binary logs in incremental backup")
	}

	getPurgedGTIDSet := func() (replication.Position, replication.Mysql56GTIDSet, error) {
		gtidPurged, err := params.Mysqld.GetGTIDPurged(ctx)
		if err != nil {
			return gtidPurged, nil, vterrors.Wrap(err, "can't get @@gtid_purged")
		}
		purgedGTIDSet, ok := gtidPurged.GTIDSet.(replication.Mysql56GTIDSet)
		if !ok {
			return gtidPurged, nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "failed to parse a valid MySQL GTID set from value: %v", gtidPurged)
		}
		return gtidPurged, purgedGTIDSet, nil
	}
	// gtid_purged is important information. The restore flow uses this info to to complement binary logs' Previous-GTIDs.
	// It is important to only get gtid_purged _after_ we've rotated into the new binary log, because the `FLUSH BINARY LOGS`
	// command may also purge old logs, hence affecting the value of gtid_purged.
	gtidPurged, purgedGTIDSet, err := getPurgedGTIDSet()
	if err != nil {
		return BackupUnusable, err
	}
	previousGTIDs := map[string]string{}
	getBinlogPreviousGTIDs := func(ctx context.Context, binlog string) (gtids string, err error) {
		gtids, ok := previousGTIDs[binlog]
		if ok {
			// Found a cached entry! No need to query again
			return gtids, nil
		}
		gtids, err = params.Mysqld.GetPreviousGTIDs(ctx, binlog)
		if err != nil {
			return gtids, err
		}
		previousGTIDs[binlog] = gtids
		return gtids, nil
	}
	binaryLogsToBackup, incrementalBackupFromGTID, incrementalBackupToGTID, err := ChooseBinlogsForIncrementalBackup(ctx, backupFromGTIDSet, purgedGTIDSet, binaryLogs, getBinlogPreviousGTIDs)
	if err != nil {
		return BackupUnusable, vterrors.Wrapf(err, "cannot get binary logs to backup in incremental backup")
	}
	if len(binaryLogsToBackup) == 0 {
		// Empty backup.
		return BackupEmpty, nil
	}
	incrementalBackupFromPosition, err := replication.ParsePosition(replication.Mysql56FlavorID, incrementalBackupFromGTID)
	if err != nil {
		return BackupUnusable, vterrors.Wrapf(err, "cannot parse position %v", incrementalBackupFromGTID)
	}
	incrementalBackupToPosition, err := replication.ParsePosition(replication.Mysql56FlavorID, incrementalBackupToGTID)
	if err != nil {
		return BackupUnusable, vterrors.Wrapf(err, "cannot parse position %v", incrementalBackupToGTID)
	}
	// The backup position is the GTISset of the last binary log (taken from Previous-GTIDs of the one-next binary log), and we
	// also include gtid_purged ; this complies with the "standard" way MySQL "thinks" about GTIDs: there's gtid_executed, which includes
	// everything that's ever been applied, and a subset of that is gtid_purged, which are the event no longer available in binary logs.
	// When we consider Vitess incremental backups, what's important for us is "what's the GTIDSet that's true when this backup was taken,
	// and which will be true when we restore this backup". The answer to this is the GTIDSet that includes the purged GTIDs.
	// It's also nice for incremental backups that are taken on _other_ tablets, so that they don't need to understand what exactly was purged
	// on _this_ tablet. They don't care, all they want to know is "what GTIDSet can we get from this".
	incrementalBackupToPosition.GTIDSet = incrementalBackupToPosition.GTIDSet.Union(gtidPurged.GTIDSet)
	req := &mysqlctlpb.ReadBinlogFilesTimestampsRequest{}
	for _, binlogFile := range binaryLogsToBackup {
		fe := FileEntry{Base: backupBinlogDir, Name: binlogFile}
		fullPath, err := fe.fullPath(params.Cnf)
		if err != nil {
			return BackupUnusable, err
		}
		req.BinlogFileNames = append(req.BinlogFileNames, fullPath)
	}
	resp, err := params.Mysqld.ReadBinlogFilesTimestamps(ctx, req)
	if err != nil {
		return BackupUnusable, vterrors.Wrapf(err, "reading timestamps from binlog files %v", binaryLogsToBackup)
	}
	if resp.FirstTimestampBinlog == "" || resp.LastTimestampBinlog == "" {
		return BackupUnusable, vterrors.Errorf(vtrpcpb.Code_ABORTED, "empty binlog name in response. Request=%v, Response=%v", req, resp)
	}
	log.Infof("ReadBinlogFilesTimestampsResponse: %+v", resp)
	incrDetails := &IncrementalBackupDetails{
		FirstTimestamp:       FormatRFC3339(protoutil.TimeFromProto(resp.FirstTimestamp).UTC()),
		FirstTimestampBinlog: filepath.Base(resp.FirstTimestampBinlog),
		LastTimestamp:        FormatRFC3339(protoutil.TimeFromProto(resp.LastTimestamp).UTC()),
		LastTimestampBinlog:  filepath.Base(resp.LastTimestampBinlog),
	}
	// It's worthwhile we explain the difference between params.IncrementalFromPos and incrementalBackupFromPosition.
	// params.IncrementalFromPos is supplied by the user. They want an incremental backup that covers that position.
	// However, we implement incremental backups by copying complete binlog files. That position could potentially
	// be somewhere in the middle of some binlog. So we look at the earliest binlog file that covers the user's position.
	// The backup we take either starts exactly at the user's position or at some prior position, depending where in the
	// binlog file the user's requested position is found.
	// incrementalBackupFromGTID is the "previous GTIDs" of the first binlog file we back up.
	// It is a fact that incrementalBackupFromGTID is earlier or equal to params.IncrementalFromPos.
	// In the backup manifest file, we document incrementalBackupFromGTID, not the user's requested position.
	if err := be.backupFiles(ctx, params, bh, incrementalBackupToPosition, gtidPurged, incrementalBackupFromPosition, fromBackupName, binaryLogsToBackup, serverUUID, mysqlVersion, incrDetails); err != nil {
		return BackupUnusable, err
	}
	return BackupUsable, nil
}

// executeFullBackup returns a BackupResult that indicates the usability of the backup,
// and an overall error.
func (be *BuiltinBackupEngine) executeFullBackup(ctx context.Context, params BackupParams, bh backupstorage.BackupHandle) (BackupResult, error) {
	if params.IncrementalFromPos != "" {
		return be.executeIncrementalBackup(ctx, params, bh)
	}

	// Save initial state so we can restore.
	replicaStartRequired := false
	sourceIsPrimary := false
	superReadOnly := true
	var replicationPosition replication.Position
	semiSyncSource, semiSyncReplica := params.Mysqld.SemiSyncEnabled(ctx)

	// See if we need to restart replication after backup.
	params.Logger.Infof("getting current replication status")
	replicaStatus, err := params.Mysqld.ReplicationStatus(ctx)
	switch err {
	case nil:
		replicaStartRequired = replicaStatus.Healthy() && !DisableActiveReparents
	case mysql.ErrNotReplica:
		// keep going if we're the primary, might be a degenerate case
		sourceIsPrimary = true
	default:
		return BackupUnusable, vterrors.Wrap(err, "can't get replica status")
	}

	// get the read-only flag
	readOnly, err := params.Mysqld.IsReadOnly(ctx)
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "failed to get read_only status")
	}
	superReadOnly, err = params.Mysqld.IsSuperReadOnly(ctx)
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "can't get super_read_only status")
	}
	log.Infof("Flag values during full backup, read_only: %v, super_read_only:%t", readOnly, superReadOnly)

	// get the replication position
	if sourceIsPrimary {
		// No need to set read_only because super_read_only will implicitly set read_only to true as well.
		if !superReadOnly {
			params.Logger.Infof("Enabling super_read_only on primary prior to backup")
			if _, err = params.Mysqld.SetSuperReadOnly(ctx, true); err != nil {
				return BackupUnusable, vterrors.Wrap(err, "failed to enable super_read_only")
			}
			defer func() {
				// Resetting super_read_only back to its original value
				params.Logger.Infof("resetting mysqld super_read_only to %v", superReadOnly)
				if _, err := params.Mysqld.SetSuperReadOnly(ctx, false); err != nil {
					log.Error("Failed to set super_read_only back to its original value")
				}
			}()
		}
		replicationPosition, err = params.Mysqld.PrimaryPosition(ctx)
		if err != nil {
			return BackupUnusable, vterrors.Wrap(err, "can't get position on primary")
		}
	} else {
		// This is a replica
		if err := params.Mysqld.StopReplication(ctx, params.HookExtraEnv); err != nil {
			return BackupUnusable, vterrors.Wrapf(err, "can't stop replica")
		}
		replicaStatus, err := params.Mysqld.ReplicationStatus(ctx)
		if err != nil {
			return BackupUnusable, vterrors.Wrap(err, "can't get replica status")
		}
		replicationPosition = replicaStatus.Position
	}
	params.Logger.Infof("using replication position: %v", replicationPosition)

	gtidPurgedPosition, err := params.Mysqld.GetGTIDPurged(ctx)
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "can't get gtid_purged")
	}

	serverUUID, err := params.Mysqld.GetServerUUID(ctx)
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "can't get server uuid")
	}

	mysqlVersion, err := params.Mysqld.GetVersionString(ctx)
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "can't get MySQL version")
	}

	// check if we need to set innodb_fast_shutdown=0 for a backup safe for upgrades
	if params.UpgradeSafe {
		if _, err := params.Mysqld.FetchSuperQuery(ctx, "SET GLOBAL innodb_fast_shutdown=0"); err != nil {
			return BackupUnusable, vterrors.Wrapf(err, "failed to disable fast shutdown")
		}
	}

	// shutdown mysqld
	shutdownCtx, cancel := context.WithTimeout(ctx, BuiltinBackupMysqldTimeout)
	err = params.Mysqld.Shutdown(shutdownCtx, params.Cnf, true, params.MysqlShutdownTimeout)
	defer cancel()
	if err != nil {
		return BackupUnusable, vterrors.Wrap(err, "can't shutdown mysqld")
	}

	// Backup everything, capture the error.
	backupErr := be.backupFiles(ctx, params, bh, replicationPosition, gtidPurgedPosition, replication.Position{}, "", nil, serverUUID, mysqlVersion, nil)
	backupResult := BackupUnusable
	if backupErr == nil {
		backupResult = BackupUsable
	}

	// Try to restart mysqld, use background context in case we timed out the original context
	err = params.Mysqld.Start(context.Background(), params.Cnf)
	if err != nil {
		return backupResult, vterrors.Wrap(err, "can't restart mysqld")
	}

	// Resetting super_read_only back to its original value
	params.Logger.Infof("resetting mysqld super_read_only to %v", superReadOnly)
	if _, err := params.Mysqld.SetSuperReadOnly(ctx, superReadOnly); err != nil {
		return backupResult, err
	}

	// Restore original mysqld state that we saved above.
	if semiSyncSource || semiSyncReplica {
		// Only do this if one of them was on, since both being off could mean
		// the plugin isn't even loaded, and the server variables don't exist.
		params.Logger.Infof("restoring semi-sync settings from before backup: primary=%v, replica=%v",
			semiSyncSource, semiSyncReplica)
		err := params.Mysqld.SetSemiSyncEnabled(ctx, semiSyncSource, semiSyncReplica)
		if err != nil {
			return backupResult, err
		}
	}
	if replicaStartRequired {
		params.Logger.Infof("restarting mysql replication")
		if err := params.Mysqld.StartReplication(ctx, params.HookExtraEnv); err != nil {
			return backupResult, vterrors.Wrap(err, "cannot restart replica")
		}

		// this should be quick, but we might as well just wait
		if err := WaitForReplicationStart(ctx, params.Mysqld, replicationStartDeadline); err != nil {
			return backupResult, vterrors.Wrap(err, "replica is not restarting")
		}

		// Wait for a reliable value for ReplicationLagSeconds from ReplicationStatus()

		// We know that we stopped at replicationPosition.
		// If PrimaryPosition is the same, that means no writes
		// have happened to primary, so we are up-to-date.
		// Otherwise, we wait for replica's Position to change from
		// the saved replicationPosition before proceeding
		tmc := tmclient.NewTabletManagerClient()
		defer tmc.Close()
		remoteCtx, remoteCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
		defer remoteCancel()

		pos, err := getPrimaryPosition(remoteCtx, tmc, params.TopoServer, params.Keyspace, params.Shard)
		// If we are unable to get the primary's position, return error.
		if err != nil {
			return backupResult, err
		}
		if !replicationPosition.Equal(pos) {
			for {
				if err := ctx.Err(); err != nil {
					return backupResult, err
				}
				status, err := params.Mysqld.ReplicationStatus(ctx)
				if err != nil {
					return backupResult, err
				}
				newPos := status.Position
				if !newPos.Equal(replicationPosition) {
					break
				}
				time.Sleep(1 * time.Second)
			}
		}
	}

	return backupResult, backupErr
}

// backupFiles finds the list of files to backup, and creates the backup.
func (be *BuiltinBackupEngine) backupFiles(
	ctx context.Context,
	params BackupParams,
	bh backupstorage.BackupHandle,
	backupPosition replication.Position,
	purgedPosition replication.Position,
	fromPosition replication.Position,
	fromBackupName string,
	binlogFiles []string,
	serverUUID string,
	mysqlVersion string,
	incrDetails *IncrementalBackupDetails,
) (finalErr error) {
	// backupFiles always wait for AddFiles to finish its work before returning, unless there has been a
	// non-recoverable error in the process, in both cases we can cancel the context safely.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Get the files to backup.
	// We don't care about totalSize because we add each file separately.
	var fes []FileEntry
	var err error
	if isIncrementalBackup(params) {
		fes, _, err = binlogFilesToBackup(params.Cnf, binlogFiles)
	} else {
		fes, _, err = findFilesToBackup(params.Cnf)
	}
	if err != nil {
		return vterrors.Wrap(err, "can't find files to backup")
	}
	params.Logger.Infof("found %v files to backup", len(fes))

	// The error here can be ignored safely. Failed FileEntry's are handled in the next 'if' statement.
	_ = be.backupFileEntries(ctx, fes, bh, params)

	// BackupHandle supports the BackupErrorRecorder interface for tracking errors
	// across any goroutines that fan out to take the backup. This means that we
	// don't need a local error recorder and can put everything through the bh.
	//
	// This handles the scenario where bh.AddFile() encounters an error asynchronously,
	// which ordinarily would be lost in the context of `be.backupFile`, i.e. if an
	// error were encountered
	// [here](https://github.com/vitessio/vitess/blob/d26b6c7975b12a87364e471e2e2dfa4e253c2a5b/go/vt/mysqlctl/s3backupstorage/s3.go#L139-L142).
	//
	// All the errors are grouped per file, if one or more files failed, we back them up
	// once more concurrently, if any of the retry fail, we fail-fast by canceling the context
	// and return an error. There is no reason to continue processing the other retries, if
	// one of them failed.
	if files := bh.GetFailedFiles(); len(files) > 0 {
		newFEs := make([]FileEntry, len(fes))
		for _, file := range files {
			fileNb, err := strconv.Atoi(file)
			if err != nil {
				return vterrors.Wrapf(err, "failed to retry file '%s'", file)
			}
			oldFes := fes[fileNb]
			newFEs[fileNb] = FileEntry{
				Base:       oldFes.Base,
				Name:       oldFes.Name,
				ParentPath: oldFes.ParentPath,
				RetryCount: 1,
			}
			bh.ResetErrorForFile(file)
		}
		err = be.backupFileEntries(ctx, newFEs, bh, params)
		if err != nil {
			return err
		}
	}

	// Backup the MANIFEST file and apply retry logic.
	var manifestErr error
	for currentRetry := 0; currentRetry <= maxRetriesPerFile; currentRetry++ {
		manifestErr = be.backupManifest(ctx, params, bh, backupPosition, purgedPosition, fromPosition, fromBackupName, serverUUID, mysqlVersion, incrDetails, fes, currentRetry)
		if manifestErr == nil || vterrors.Code(manifestErr) == vtrpcpb.Code_FAILED_PRECONDITION {
			break
		}
		bh.ResetErrorForFile(backupManifestFileName)
	}
	if manifestErr != nil {
		return manifestErr
	}
	return nil
}

// backupFileEntries iterates over a slice of FileEntry, backing them up concurrently up to the defined concurrency limit.
// This function will ignore empty FileEntry, allowing the retry mechanism to send a partially empty slice, to not
// mess up the index of retriable FileEntry.
// This function does not leave any background operation behind itself, all calls to bh.AddFile will be finished or canceled.
func (be *BuiltinBackupEngine) backupFileEntries(ctx context.Context, fes []FileEntry, bh backupstorage.BackupHandle, params BackupParams) error {
	ctxCancel, cancel := context.WithCancel(ctx)
	defer func() {
		// If we reached this defer in all cases we can cancel the context.
		// The only ways to get here are: a panic, an error when ending the backup, a successful backup.
		// For all three options, it is safe to cancel the context, there should be no pending operations
		// that 1) haven't completed, 2) we care about anymore.
		cancel()
	}()

	g := errgroup.Group{}
	g.SetLimit(params.Concurrency)
	for i := range fes {
		if fes[i].Name == "" {
			continue
		}
		g.Go(func() error {
			fe := &fes[i]
			name := strconv.Itoa(i)

			// Check for context cancellation explicitly because, the way semaphore code is written, theoretically we might
			// end up not throwing an error even after cancellation. Please see https://cs.opensource.google/go/x/sync/+/refs/tags/v0.1.0:semaphore/semaphore.go;l=66,
			// which suggests that if the context is already done, `Acquire()` may still succeed without blocking. This introduces
			// unpredictability in my test cases, so in order to avoid that, I am adding this cancellation check.
			select {
			case <-ctxCancel.Done():
				log.Errorf("Context canceled or timed out during %q backup", fe.Name)
				bh.RecordError(name, vterrors.Errorf(vtrpcpb.Code_CANCELED, "context canceled"))
				return nil
			default:
			}

			// Backup the individual file.
			var errBackupFile error
			if errBackupFile = be.backupFile(ctxCancel, params, bh, fe, name); errBackupFile != nil {
				bh.RecordError(name, vterrors.Wrapf(errBackupFile, "failed to backup file '%s'", name))
				if fe.RetryCount >= maxRetriesPerFile || vterrors.Code(errBackupFile) == vtrpcpb.Code_FAILED_PRECONDITION {
					// this is the last attempt, and we have an error, we can cancel everything and fail fast.
					cancel()
				}
			}
			return nil
		})
	}
	_ = g.Wait()

	err := bh.EndBackup(ctx)
	if err != nil {
		return err
	}
	return bh.Error()
}

type backupPipe struct {
	filename string
	maxSize  int64

	r io.Reader
	w *bufio.Writer

	crc32  hash.Hash32
	nn     int64
	done   chan struct{}
	failed chan struct{}
	closed int32
}

func newBackupWriter(filename string, writerBufferSize int, maxSize int64, w io.Writer) *backupPipe {
	return &backupPipe{
		crc32:    crc32.NewIEEE(),
		w:        bufio.NewWriterSize(w, writerBufferSize),
		filename: filename,
		maxSize:  maxSize,
		done:     make(chan struct{}),
		failed:   make(chan struct{}),
	}
}

func newBackupReader(filename string, maxSize int64, r io.Reader) *backupPipe {
	return &backupPipe{
		crc32:    crc32.NewIEEE(),
		r:        r,
		filename: filename,
		done:     make(chan struct{}),
		failed:   make(chan struct{}),
		maxSize:  maxSize,
	}
}

func retryToString(retry int) string {
	// We convert the retry number to an attempt number, increasing retry by one, so it looks more human friendly
	return fmt.Sprintf("(attempt %d/%d)", retry+1, maxRetriesPerFile+1)
}

func (bp *backupPipe) Read(p []byte) (int, error) {
	nn, err := bp.r.Read(p)
	_, _ = bp.crc32.Write(p[:nn])
	atomic.AddInt64(&bp.nn, int64(nn))
	return nn, err
}

func (bp *backupPipe) Write(p []byte) (int, error) {
	nn, err := bp.w.Write(p)
	_, _ = bp.crc32.Write(p[:nn])
	atomic.AddInt64(&bp.nn, int64(nn))
	return nn, err
}

func (bp *backupPipe) Close(isDone bool) (err error) {
	if atomic.CompareAndSwapInt32(&bp.closed, 0, 1) {
		// If we fail to Flush the writer we must report this backup as a failure.
		defer func() {
			if isDone && err == nil {
				close(bp.done)
				return
			}
			close(bp.failed)
		}()

		if bp.w != nil {
			if err := bp.w.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (bp *backupPipe) HashString() string {
	return hex.EncodeToString(bp.crc32.Sum(nil))
}

func (bp *backupPipe) ReportProgress(ctx context.Context, period time.Duration, logger logutil.Logger, restore bool, retryStr string) {
	messageStr := "restoring"
	if !restore {
		messageStr = "backing up"
	}
	tick := time.NewTicker(period)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Infof("Canceled %s of %q file %s", messageStr, bp.filename, retryStr)
			return
		case <-bp.done:
			logger.Infof("Completed %s %q %s", messageStr, bp.filename, retryStr)
			return
		case <-bp.failed:
			logger.Infof("Failed %s %q %s", messageStr, bp.filename, retryStr)
			return
		case <-tick.C:
			written := float64(atomic.LoadInt64(&bp.nn))
			if bp.maxSize == 0 {
				logger.Infof("%s %q %s: %.02fkb", messageStr, bp.filename, retryStr, written/1024.0)
			} else {
				maxSize := float64(bp.maxSize)
				logger.Infof("%s %q %s: %.02f%% (%.02f/%.02fkb)", messageStr, bp.filename, retryStr, 100.0*written/maxSize, written/1024.0, maxSize/1024.0)
			}
		}
	}
}

// backupFile backs up an individual file.
func (be *BuiltinBackupEngine) backupFile(ctx context.Context, params BackupParams, bh backupstorage.BackupHandle, fe *FileEntry, name string) (finalErr error) {
	// We need another context that does not live outside of this function.
	// Reporting progress, compressing and writing are operations that will be
	// over by the time we exit this function, they can use this cancelable context.
	// However, AddFile is something that may continue in the background even after
	// this function exits. In this case, we give it the parent context so the caller
	// has more control over when to cancel AddFile.
	cancelableCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Open the source file for reading.
	openSourceAt := time.Now()
	source, err := fe.open(params.Cnf, true)
	if err != nil {
		return err
	}
	params.Stats.Scope(stats.Operation("Source:Open")).TimedIncrement(time.Since(openSourceAt))

	defer func() {
		closeSourceAt := time.Now()
		if err := closeWithRetry(ctx, params.Logger, source, fe.Name); err != nil {
			params.Logger.Infof("Failed to close %s source file during backup: %v", fe.Name, err)
			return
		}
		params.Stats.Scope(stats.Operation("Source:Close")).TimedIncrement(time.Since(closeSourceAt))
	}()

	readStats := params.Stats.Scope(stats.Operation("Source:Read"))
	timedSource := ioutil.NewMeteredReadCloser(source, readStats.TimedIncrementBytes)

	fi, err := source.Stat()
	if err != nil {
		return err
	}

	retryStr := retryToString(fe.RetryCount)
	br := newBackupReader(fe.Name, fi.Size(), timedSource)
	go br.ReportProgress(cancelableCtx, builtinBackupProgress, params.Logger, false /*restore*/, retryStr)

	// Open the destination file for writing, and a buffer.
	params.Logger.Infof("Backing up file: %v %s", fe.Name, retryStr)
	openDestAt := time.Now()
	dest, err := bh.AddFile(ctx, name, fi.Size())
	if err != nil {
		return vterrors.Wrapf(err, "cannot add file: %v,%v", name, fe.Name)
	}
	params.Stats.Scope(stats.Operation("Destination:Open")).TimedIncrement(time.Since(openDestAt))

	defer func(name, fileName string) {
		closeDestAt := time.Now()
		if rerr := closeWithRetry(ctx, params.Logger, dest, fe.Name); rerr != nil {
			rerr = vterrors.Wrapf(rerr, "failed to close destination file (%v) %v", name, fe.Name)
			params.Logger.Error(rerr)
			finalErr = errors.Join(finalErr, rerr)
			return
		}
		params.Stats.Scope(stats.Operation("Destination:Close")).TimedIncrement(time.Since(closeDestAt))
	}(name, fe.Name)

	destStats := params.Stats.Scope(stats.Operation("Destination:Write"))
	timedDest := ioutil.NewMeteredWriteCloser(dest, destStats.TimedIncrementBytes)

	bw := newBackupWriter(fe.Name, builtinBackupStorageWriteBufferSize, fi.Size(), timedDest)

	// We create the following inner function because:
	// - we must `defer` the compressor's Close() function
	// - but it must take place before we close the pipe reader&writer
	createAndCopy := func() (createAndCopyErr error) {
		var reader io.Reader = br
		var writer io.Writer = bw

		defer func() {
			// Close the backupPipe to finish writing on destination.
			if err := bw.Close(createAndCopyErr == nil); err != nil {
				createAndCopyErr = errors.Join(createAndCopyErr, vterrors.Wrapf(err, "cannot flush destination: %v", name))
			}

			if err := br.Close(createAndCopyErr == nil); err != nil {
				createAndCopyErr = errors.Join(createAndCopyErr, vterrors.Wrap(err, "failed to close the source reader"))
			}
		}()
		// Create the gzip compression pipe, if necessary.
		if backupStorageCompress {
			var compressor io.WriteCloser
			if ExternalCompressorCmd != "" {
				compressor, err = newExternalCompressor(cancelableCtx, ExternalCompressorCmd, writer, params.Logger)
			} else {
				compressor, err = newBuiltinCompressor(CompressionEngineName, writer, params.Logger)
			}
			if err != nil {
				return vterrors.Wrap(err, "can't create compressor")
			}

			compressStats := params.Stats.Scope(stats.Operation("Compressor:Write"))
			writer = ioutil.NewMeteredWriter(compressor, compressStats.TimedIncrementBytes)

			closer := ioutil.NewTimeoutCloser(cancelableCtx, compressor, closeTimeout)
			defer func() {
				// Close gzip to flush it, after that all data is sent to writer.
				params.Logger.Infof("Closing compressor for file: %s %s", fe.Name, retryStr)
				closeCompressorAt := time.Now()
				if cerr := closeWithRetry(ctx, params.Logger, closer, "compressor"); cerr != nil {
					cerr = vterrors.Wrapf(cerr, "failed to close compressor %v", fe.Name)
					params.Logger.Error(cerr)
					createAndCopyErr = errors.Join(createAndCopyErr, cerr)
					return
				}
				params.Stats.Scope(stats.Operation("Compressor:Close")).TimedIncrement(time.Since(closeCompressorAt))
			}()
		}

		if builtinBackupFileReadBufferSize > 0 {
			reader = bufio.NewReaderSize(br, int(builtinBackupFileReadBufferSize))
		}

		// Copy from the source file to writer (optional gzip,
		// optional pipe, tee, output file and hasher).
		_, err = io.Copy(writer, reader)
		if err != nil {
			return vterrors.Wrap(err, "cannot copy data")
		}
		return nil
	}

	if err := createAndCopy(); err != nil {
		return errors.Join(finalErr, err)
	}

	// Save the hash.
	fe.Hash = bw.HashString()
	return nil
}

func (be *BuiltinBackupEngine) backupManifest(
	ctx context.Context,
	params BackupParams,
	bh backupstorage.BackupHandle,
	backupPosition replication.Position,
	purgedPosition replication.Position,
	fromPosition replication.Position,
	fromBackupName string,
	serverUUID string,
	mysqlVersion string,
	incrDetails *IncrementalBackupDetails,
	fes []FileEntry,
	currentAttempt int,
) (finalErr error) {
	retryStr := retryToString(currentAttempt)
	params.Logger.Infof("Backing up file %s %s", backupManifestFileName, retryStr)
	defer func() {
		state := "Completed"
		if finalErr != nil {
			state = "Failed"
		}
		params.Logger.Infof("%s backing up %s %s", state, backupManifestFileName, retryStr)
	}()

	// Creating this function allows us to ensure we always close the writer no matter what,
	// and in case of success that we close it before calling bh.EndBackup.
	addAndWrite := func() (addAndWriteError error) {
		// open the MANIFEST
		wc, err := bh.AddFile(ctx, backupManifestFileName, backupstorage.FileSizeUnknown)
		if err != nil {
			return vterrors.Wrapf(err, "cannot add %v to backup %s", backupManifestFileName, retryStr)
		}
		defer func() {
			if err := closeWithRetry(ctx, params.Logger, wc, backupManifestFileName); err != nil {
				addAndWriteError = errors.Join(addAndWriteError, vterrors.Wrapf(err, "cannot close backup: %v", backupManifestFileName))
			}
		}()

		// Get the hostname
		hostname, err := netutil.FullyQualifiedHostname()
		if err != nil {
			hostname = ""
		}

		// JSON-encode and write the MANIFEST
		bm := &builtinBackupManifest{
			// Common base fields
			BackupManifest: BackupManifest{
				BackupName:         bh.Name(),
				BackupMethod:       builtinBackupEngineName,
				Position:           backupPosition,
				PurgedPosition:     purgedPosition,
				FromPosition:       fromPosition,
				FromBackup:         fromBackupName,
				Incremental:        !fromPosition.IsZero(),
				ServerUUID:         serverUUID,
				TabletAlias:        params.TabletAlias,
				Hostname:           hostname,
				Keyspace:           params.Keyspace,
				Shard:              params.Shard,
				BackupTime:         params.BackupTime.UTC().Format(time.RFC3339),
				FinishedTime:       time.Now().UTC().Format(time.RFC3339),
				MySQLVersion:       mysqlVersion,
				UpgradeSafe:        params.UpgradeSafe,
				IncrementalDetails: incrDetails,
			},

			// Builtin-specific fields
			FileEntries:          fes,
			SkipCompress:         !backupStorageCompress,
			CompressionEngine:    CompressionEngineName,
			ExternalDecompressor: ManifestExternalDecompressorCmd,
		}
		data, err := json.MarshalIndent(bm, "", "  ")
		if err != nil {
			return vterrors.Wrapf(err, "cannot JSON encode %v %s", backupManifestFileName, retryStr)
		}
		if _, err := wc.Write(data); err != nil {
			return vterrors.Wrapf(err, "cannot write %v %s", backupManifestFileName, retryStr)
		}
		return nil
	}

	err := addAndWrite()
	if err != nil {
		return err
	}

	err = bh.EndBackup(ctx)
	if err != nil {
		return err
	}
	return bh.Error()
}

// executeRestoreFullBackup restores the files from a full backup. The underlying mysql database service is expected to be stopped.
func (be *BuiltinBackupEngine) executeRestoreFullBackup(ctx context.Context, params RestoreParams, bh backupstorage.BackupHandle, bm builtinBackupManifest) error {
	if err := prepareToRestore(ctx, params.Cnf, params.Mysqld, params.Logger, params.MysqlShutdownTimeout); err != nil {
		return err
	}

	params.Logger.Infof("Restore: copying %v files", len(bm.FileEntries))

	if _, err := be.restoreFiles(ctx, params, bh, bm); err != nil {
		// don't delete the file here because that is how we detect an interrupted restore
		return vterrors.Wrap(err, "failed to restore files")
	}
	return nil
}

// executeRestoreIncrementalBackup executes a restore of an incremental backup, and expect to run on top of a full backup's restore.
// It restores any (zero or more) binary log files and applies them onto the underlying database one at a time, but only applies those transactions
// that fall within params.RestoreToPos.GTIDSet. The rest (typically a suffix of the last binary log) are discarded.
// The underlying mysql database is expected to be up and running.
func (be *BuiltinBackupEngine) executeRestoreIncrementalBackup(ctx context.Context, params RestoreParams, bh backupstorage.BackupHandle, bm builtinBackupManifest) error {
	params.Logger.Infof("Restoring incremental backup to position: %v", bm.Position)
	createdDir, err := be.restoreFiles(ctx, params, bh, bm)
	defer os.RemoveAll(createdDir)
	mysqld, ok := params.Mysqld.(*Mysqld)
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "expected: Mysqld")
	}
	for _, fe := range bm.FileEntries {
		fe.ParentPath = createdDir
		binlogFile, err := fe.fullPath(params.Cnf)
		if err != nil {
			return vterrors.Wrap(err, "failed to restore file")
		}
		req := &mysqlctlpb.ApplyBinlogFileRequest{
			BinlogFileName:        binlogFile,
			BinlogRestoreDatetime: protoutil.TimeToProto(params.RestoreToTimestamp),
		}
		if params.RestoreToPos.GTIDSet != nil {
			req.BinlogRestorePosition = params.RestoreToPos.GTIDSet.String()
		}
		if err := mysqld.ApplyBinlogFile(ctx, req); err != nil {
			return vterrors.Wrapf(err, "failed to apply binlog file %v", binlogFile)
		}
		defer os.Remove(binlogFile)
		params.Logger.Infof("Applied binlog file: %v", binlogFile)
	}
	if err != nil {
		// don't delete the file here because that is how we detect an interrupted restore
		return vterrors.Wrap(err, "failed to restore files")
	}
	params.Logger.Infof("Restored incremental backup files to: %v", createdDir)

	return nil
}

// ExecuteRestore restores from a backup. If the restore is successful
// we return the position from which replication should start
// otherwise an error is returned
func (be *BuiltinBackupEngine) ExecuteRestore(ctx context.Context, params RestoreParams, bh backupstorage.BackupHandle) (*BackupManifest, error) {
	bm, err := be.restoreManifest(ctx, params, bh)
	if err != nil {
		return nil, err
	}

	// mark restore as in progress
	if err := createStateFile(params.Cnf); err != nil {
		return nil, err
	}

	if bm.Incremental {
		err = be.executeRestoreIncrementalBackup(ctx, params, bh, bm)
	} else {
		err = be.executeRestoreFullBackup(ctx, params, bh, bm)
	}
	if err != nil {
		return nil, err
	}
	params.Logger.Infof("Restore: returning replication position %v", bm.Position)
	return &bm.BackupManifest, nil
}

func (be *BuiltinBackupEngine) restoreManifest(ctx context.Context, params RestoreParams, bh backupstorage.BackupHandle) (bm builtinBackupManifest, finalErr error) {
	var retryCount int
	defer func() {
		state := "Completed"
		if finalErr != nil {
			state = "Failed"
		}
		params.Logger.Infof("%s restoring %s %s", state, backupManifestFileName, retryToString(retryCount))
	}()

	for ; retryCount <= maxRetriesPerFile; retryCount++ {
		params.Logger.Infof("Restoring file %s %s", backupManifestFileName, retryToString(retryCount))
		if finalErr = getBackupManifestInto(ctx, bh, &bm); finalErr == nil || vterrors.Code(finalErr) == vtrpcpb.Code_FAILED_PRECONDITION {
			break
		}
		params.Logger.Infof("Failed restoring %s %s", backupManifestFileName, retryToString(retryCount))
	}
	return
}

// restoreFiles will copy all the files from the BackupStorage to the
// right place.
func (be *BuiltinBackupEngine) restoreFiles(ctx context.Context, params RestoreParams, bh backupstorage.BackupHandle, bm builtinBackupManifest) (createdDir string, err error) {
	// For optimization, we are replacing pargzip with pgzip, so newBuiltinDecompressor doesn't have to compare and print warning for every file
	// since newBuiltinDecompressor is helper method and does not hold any state, it was hard to do it in that method itself.
	if bm.CompressionEngine == PargzipCompressor {
		params.Logger.Warningf(`engine "pargzip" doesn't support decompression, using "pgzip" instead`)
		bm.CompressionEngine = PgzipCompressor
		defer func() {
			bm.CompressionEngine = PargzipCompressor
		}()
	}

	if bm.Incremental {
		createdDir, err = os.MkdirTemp(builtinIncrementalRestorePath, "restore-incremental-*")
		if err != nil {
			return "", err
		}
	}
	fes := bm.FileEntries
	_ = be.restoreFileEntries(ctx, fes, bh, bm, params, createdDir)
	if files := bh.GetFailedFiles(); len(files) > 0 {
		newFEs := make([]FileEntry, len(fes))
		for _, file := range files {
			fileNb, err := strconv.Atoi(file)
			if err != nil {
				return "", vterrors.Wrapf(err, "failed to retry file '%s'", file)
			}
			oldFes := fes[fileNb]
			newFEs[fileNb] = FileEntry{
				Base:       oldFes.Base,
				Name:       oldFes.Name,
				ParentPath: oldFes.ParentPath,
				Hash:       oldFes.Hash,
				RetryCount: 1,
			}
			bh.ResetErrorForFile(file)
		}
		err = be.restoreFileEntries(ctx, newFEs, bh, bm, params, createdDir)
		if err != nil {
			return "", err
		}
	}
	return createdDir, nil
}

func (be *BuiltinBackupEngine) restoreFileEntries(ctx context.Context, fes []FileEntry, bh backupstorage.BackupHandle, bm builtinBackupManifest, params RestoreParams, createdDir string) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(params.Concurrency)

	for i := range fes {
		if fes[i].Name == "" {
			continue
		}
		g.Go(func() error {
			fe := &fes[i]
			name := strconv.Itoa(i)
			// Check for context cancellation explicitly because, the way semaphore code is written, theoretically we might
			// end up not throwing an error even after cancellation. Please see https://cs.opensource.google/go/x/sync/+/refs/tags/v0.1.0:semaphore/semaphore.go;l=66,
			// which suggests that if the context is already done, `Acquire()` may still succeed without blocking. This introduces
			// unpredictability in my test cases, so in order to avoid that, I am adding this cancellation check.
			select {
			case <-ctx.Done():
				log.Errorf("Context canceled or timed out during %q restore", fe.Name)
				bh.RecordError(name, vterrors.Errorf(vtrpcpb.Code_CANCELED, "context canceled"))
				return nil
			default:
			}

			fe.ParentPath = createdDir

			// And restore the file.
			params.Logger.Infof("Copying file %v: %v %s", name, fe.Name, retryToString(fe.RetryCount))
			if errRestore := be.restoreFile(ctx, params, bh, fe, bm, name); errRestore != nil {
				bh.RecordError(name, vterrors.Wrapf(errRestore, "failed to restore file %v to %v", name, fe.Name))
				if fe.RetryCount >= maxRetriesPerFile || vterrors.Code(errRestore) == vtrpcpb.Code_FAILED_PRECONDITION {
					// this is the last attempt, and we have an error, we can return an error, which will let errgroup
					// know it can cancel the context
					return errRestore
				}
			}
			return nil
		})
	}
	_ = g.Wait()
	return bh.Error()
}

// restoreFile restores an individual file.
func (be *BuiltinBackupEngine) restoreFile(ctx context.Context, params RestoreParams, bh backupstorage.BackupHandle, fe *FileEntry, bm builtinBackupManifest, name string) (finalErr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Open the source file for reading.
	openSourceAt := time.Now()
	source, err := bh.ReadFile(ctx, name)
	if err != nil {
		return vterrors.Wrap(err, "can't open source file for reading")
	}
	params.Stats.Scope(stats.Operation("Source:Open")).TimedIncrement(time.Since(openSourceAt))

	readStats := params.Stats.Scope(stats.Operation("Source:Read"))
	timedSource := ioutil.NewMeteredReader(source, readStats.TimedIncrementBytes)

	defer func() {
		closeSourceAt := time.Now()
		if err := closeWithRetry(ctx, params.Logger, source, fe.Name); err != nil {
			params.Logger.Errorf("Failed to close source file %s during restore: %v", name, err)
			return
		}
		params.Stats.Scope(stats.Operation("Source:Close")).TimedIncrement(time.Since(closeSourceAt))
	}()

	// Create the backup/source reader and start reporting progress
	retryStr := retryToString(fe.RetryCount)
	br := newBackupReader(fe.Name, 0, timedSource)
	go br.ReportProgress(ctx, builtinBackupProgress, params.Logger, true, retryStr)
	defer func() {
		if err := br.Close(finalErr == nil); err != nil {
			finalErr = vterrors.Wrap(finalErr, "failed to close the source reader")
		}
	}()
	var reader io.Reader = br

	// Open the destination file for writing.
	openDestAt := time.Now()
	dest, err := fe.open(params.Cnf, false)
	if err != nil {
		return vterrors.Wrap(err, "can't open destination file for writing")
	}
	params.Stats.Scope(stats.Operation("Destination:Open")).TimedIncrement(time.Since(openDestAt))

	defer func() {
		closeDestAt := time.Now()
		if cerr := closeWithRetry(ctx, params.Logger, dest, fe.Name); cerr != nil {
			finalErr = errors.Join(finalErr, vterrors.Wrap(cerr, "failed to close destination file"))
			params.Logger.Errorf("Failed to close destination file %s during restore: %v", dest.Name(), cerr)
			return
		}
		params.Stats.Scope(stats.Operation("Destination:Close")).TimedIncrement(time.Since(closeDestAt))
	}()

	writeStats := params.Stats.Scope(stats.Operation("Destination:Write"))
	timedDest := ioutil.NewMeteredWriter(dest, writeStats.TimedIncrementBytes)

	bufferedDest := bufio.NewWriterSize(timedDest, int(builtinBackupFileWriteBufferSize))

	// Create the uncompresser if needed.
	if !bm.SkipCompress {
		var decompressor io.ReadCloser
		deCompressionEngine := bm.CompressionEngine

		if deCompressionEngine == "" {
			// for backward compatibility
			deCompressionEngine = PgzipCompressor
		}
		externalDecompressorCmd := ExternalDecompressorCmd
		if externalDecompressorCmd == "" && bm.ExternalDecompressor != "" {
			externalDecompressorCmd = bm.ExternalDecompressor
		}
		if externalDecompressorCmd != "" {
			if deCompressionEngine == ExternalCompressor {
				deCompressionEngine = externalDecompressorCmd
				decompressor, err = newExternalDecompressor(ctx, deCompressionEngine, reader, params.Logger)
			} else {
				decompressor, err = newBuiltinDecompressor(deCompressionEngine, reader, params.Logger)
			}
		} else {
			if deCompressionEngine == ExternalCompressor {
				return fmt.Errorf("%w value: %q", errUnsupportedDeCompressionEngine, ExternalCompressor)
			}
			decompressor, err = newBuiltinDecompressor(deCompressionEngine, reader, params.Logger)
		}
		if err != nil {
			return vterrors.Wrap(err, "can't create decompressor")
		}
		closer := ioutil.NewTimeoutCloser(ctx, decompressor, closeTimeout)

		decompressStats := params.Stats.Scope(stats.Operation("Decompressor:Read"))
		reader = ioutil.NewMeteredReader(decompressor, decompressStats.TimedIncrementBytes)

		defer func() {
			params.Logger.Infof("closing decompressor")
			closeDecompressorAt := time.Now()
			if cerr := closeWithRetry(ctx, params.Logger, closer, "decompressor"); cerr != nil {
				cerr = vterrors.Wrapf(cerr, "failed to close decompressor %v", name)
				params.Logger.Error(cerr)
				finalErr = errors.Join(finalErr, cerr)
				return
			}
			params.Stats.Scope(stats.Operation("Decompressor:Close")).TimedIncrement(time.Since(closeDecompressorAt))
		}()
	}

	// Copy the data. Will also write to the hasher.
	if _, err := io.Copy(bufferedDest, reader); err != nil {
		return vterrors.Wrap(err, "failed to copy file contents")
	}

	// Check the hash.
	hash := br.HashString()
	if hash != fe.Hash {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "hash mismatch for %v, got %v expected %v", fe.Name, hash, fe.Hash)
	}

	// Flush the buffer.
	if err := bufferedDest.Flush(); err != nil {
		return vterrors.Wrap(err, "failed to flush destination buffer")
	}

	return nil
}

// ShouldDrainForBackup satisfies the BackupEngine interface
// backup requires query service to be stopped, hence true
func (be *BuiltinBackupEngine) ShouldDrainForBackup(req *tabletmanagerdatapb.BackupRequest) bool {
	if req != nil && req.IncrementalFromPos != "" {
		// Incremental backup: we do not drain the tablet.
		return false
	}
	return true
}

// ShouldStartMySQLAfterRestore signifies if this backup engine needs to restart MySQL once the restore is completed.
func (be *BuiltinBackupEngine) ShouldStartMySQLAfterRestore() bool {
	return true
}

func (be *BuiltinBackupEngine) Name() string { return builtinBackupEngineName }

func getPrimaryPosition(ctx context.Context, tmc tmclient.TabletManagerClient, ts *topo.Server, keyspace, shard string) (replication.Position, error) {
	si, err := ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return replication.Position{}, vterrors.Wrap(err, "can't read shard")
	}
	if topoproto.TabletAliasIsZero(si.PrimaryAlias) {
		return replication.Position{}, fmt.Errorf("shard %v/%v has no primary", keyspace, shard)
	}
	ti, err := ts.GetTablet(ctx, si.PrimaryAlias)
	if err != nil {
		return replication.Position{}, fmt.Errorf("can't get primary tablet record %v: %v", topoproto.TabletAliasString(si.PrimaryAlias), err)
	}
	posStr, err := tmc.PrimaryPosition(ctx, ti.Tablet)
	if err != nil {
		return replication.Position{}, fmt.Errorf("can't get primary replication position: %v", err)
	}
	pos, err := replication.DecodePosition(posStr)
	if err != nil {
		return replication.Position{}, fmt.Errorf("can't decode primary replication position %q: %v", posStr, err)
	}
	return pos, nil
}

func init() {
	BackupRestoreEngineMap[builtinBackupEngineName] = &BuiltinBackupEngine{}
}

// closeWithRetry does just what it says. Retrying a close operation is important as
// an error is most likely transient/ephemeral and leaving around open file descriptors
// can lead to later problems as the file may be in a sort-of uploaded state where it
// exists but has not yet been finalized (this is true for GCS). This can cause
// unexpected behavior if you retry the file while the original request is still in this
// state. Most implementations such as GCS will automatically retry operations, but close
// is one that may be left to the caller (this is true for GCS).
// We model this retry after the GCS retry implementation described here:
// https://cloud.google.com/storage/docs/retry-strategy#go
func closeWithRetry(ctx context.Context, logger logutil.Logger, file io.Closer, name string) error {
	backoff := 1 * time.Second
	backoffLimit := backoff * 30
	var err error
	retries := 0
	fileType := "source"
	if _, ok := file.(io.Writer); ok {
		fileType = "destination"
	}
	for {
		if err = file.Close(); err == nil {
			return nil
		}
		if retries == maxFileCloseRetries {
			// Let's give up as this does not appear to be transient. We cannot know
			// the full list of all transient/ephemeral errors across all backup engine
			// providers so we consider it permanent at this point. We return a
			// FAILED_PRECONDITION code which tells the upper layers not to retry as we
			// now cannot be sure that this backup would be usable when it finishes.
			logger.Errorf("Failed to close %s file %s after %d attempts, giving up: %v", fileType, name, maxFileCloseRetries, err)
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "failed to close the %s file after %d attempts, giving up", fileType, maxFileCloseRetries)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			// Exponential backoff with 2 as a factor.
			if backoff != backoffLimit {
				updatedBackoff := time.Duration(float64(backoff) * 2)
				backoff = min(updatedBackoff, backoffLimit)
			}
		}
		retries++
		logger.Errorf("Failed to close %s file %s, will perform retry %d of %d in %v: %v", fileType, name, retries, maxFileCloseRetries, backoff, err)
	}
}
