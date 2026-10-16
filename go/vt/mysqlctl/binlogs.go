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

package mysqlctl

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/os2"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// binlogSeqRE matches a binlog file's sequence suffix, as mysqld writes it.
var binlogSeqRE = regexp.MustCompile("^[0-9]{6,}$")

// binlogLocation returns the absolute binlog basename and index file that mysqld
// derives from cnf: relative paths are under the datadir, an extension on log-bin
// is dropped, and the index defaults to <basename>.index.
func binlogLocation(cnf *Mycnf) (basename, indexPath string, err error) {
	if cnf.BinLogPath == "" {
		return "", "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "log-bin is not set")
	}
	basename = absUnderDataDir(cnf, cnf.BinLogPath)
	basename = strings.TrimSuffix(basename, filepath.Ext(basename))
	switch {
	case cnf.BinLogIndexPath == "":
		indexPath = basename + ".index"
	case filepath.Ext(cnf.BinLogIndexPath) == "":
		indexPath = absUnderDataDir(cnf, cnf.BinLogIndexPath) + ".index"
	default:
		indexPath = absUnderDataDir(cnf, cnf.BinLogIndexPath)
	}
	return basename, indexPath, nil
}

func absUnderDataDir(cnf *Mycnf, p string) string {
	if filepath.IsAbs(p) {
		return filepath.Clean(p)
	}
	return filepath.Join(cnf.DataDir, p)
}

// binlogBackupVarsQuery reads the settings that decide whether the source's binlogs
// can be backed up. MySQL 5.7 and MariaDB name the replica setting log_slave_updates,
// and MySQL 8.0.26 and later also accept it as an alias.
const binlogBackupVarsQuery = "SHOW GLOBAL VARIABLES WHERE Variable_name IN ('log_bin', 'log_replica_updates', 'log_slave_updates')"

// ValidateIncludeBinlogs fails fast when IncludeBinlogs is combined with a backup that
// cannot carry the binlogs: only a full builtin backup stops mysqld cleanly, so its
// binlogs end exactly at the backup position.
func ValidateIncludeBinlogs(params BackupParams) error {
	if !params.IncludeBinlogs {
		return nil
	}
	if isIncrementalBackup(params) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "including binlogs is only supported for full backups, not incremental ones")
	}
	be, err := GetBackupEngine(params.BackupEngine)
	if err != nil {
		return vterrors.Wrap(err, "failed to find backup engine")
	}
	if be.Name() != builtinBackupEngineName {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "including binlogs is only supported with the %s backup engine, not %s", builtinBackupEngineName, be.Name())
	}
	return nil
}

// checkBinlogsBackupable refuses to back up binlogs that would not contain every
// transaction in the data: no binlog, or no log_replica_updates, since a server that
// was a replica then did not log what it replicated.
func checkBinlogsBackupable(ctx context.Context, mysqld MysqlDaemon) error {
	qr, err := mysqld.FetchSuperQuery(ctx, binlogBackupVarsQuery)
	if err != nil {
		return vterrors.Wrap(err, "cannot read binlog settings")
	}
	vars := make(map[string]string, len(qr.Rows))
	for _, row := range qr.Rows {
		if len(row) != 2 {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected result for %q: %v", binlogBackupVarsQuery, qr.Rows)
		}
		vars[row[0].ToString()] = row[1].ToString()
	}
	logBin, ok := vars["log_bin"]
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected result for %q: %v", binlogBackupVarsQuery, qr.Rows)
	}
	replicaUpdates, ok := vars["log_replica_updates"]
	if !ok {
		replicaUpdates, ok = vars["log_slave_updates"]
	}
	switch {
	case logBin != "ON":
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot include binlogs: log_bin is off")
	case !ok || replicaUpdates != "ON":
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot include binlogs: log_replica_updates is off, so this server's binlogs may lack replicated transactions")
	}
	return nil
}

// binlogFilesForFullBackup lists the binlogs in mysqld's index, in order, as entries
// named binlog.<seq> that restore under any basename. mysqld must be shut down.
func binlogFilesForFullBackup(cnf *Mycnf) ([]FileEntry, error) {
	basename, indexPath, err := binlogLocation(cnf)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot read binlog index %s", indexPath)
	}
	var fes []FileEntry
	for line := range strings.Lines(string(data)) {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		p := absUnderDataDir(cnf, line)
		seq := strings.TrimPrefix(filepath.Ext(p), ".")
		if !binlogSeqRE.MatchString(seq) || p != basename+"."+seq {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "binlog %q in %s does not match basename %s", line, indexPath, basename)
		}
		fi, err := os.Stat(p)
		if err != nil {
			return nil, vterrors.Wrapf(err, "cannot stat binlog %s", p)
		}
		fes = append(fes, FileEntry{Base: backupBinlogStripped, Name: "binlog." + seq, ModTime: fi.ModTime().UTC()})
	}
	return fes, nil
}

// restoredBinlogVarsQuery reads where mysqld looks for binlogs after a restore.
const restoredBinlogVarsQuery = "SELECT @@global.log_bin_basename, @@global.log_bin_index"

// showBinaryLogsQuery lists the binlogs mysqld knows about.
const showBinaryLogsQuery = "SHOW BINARY LOGS"

// finishBinlogRestore gives the restored binlogs their source mtimes, so that
// binlog_expire_logs_seconds still counts from when they were written, and writes
// the index that mysqld reads them from. It runs before mysqld starts.
func finishBinlogRestore(cnf *Mycnf, fes []FileEntry) error {
	_, indexPath, err := binlogLocation(cnf)
	if err != nil {
		return err
	}
	var index strings.Builder
	for i := range fes {
		if fes[i].ModTime.IsZero() {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "binlog entry %q has no modification time", fes[i].Name)
		}
		p, err := fes[i].fullPath(cnf)
		if err != nil {
			return err
		}
		if err := os.Chtimes(p, fes[i].ModTime, fes[i].ModTime); err != nil {
			return vterrors.Wrapf(err, "cannot set modification time of %s", p)
		}
		index.WriteString(p + "\n")
	}
	// A log-bin-index outside the restored directories may live in a directory the
	// restore removed with the datadir.
	if err := os2.MkdirAll(filepath.Dir(indexPath)); err != nil {
		return vterrors.Wrapf(err, "cannot create the directory of binlog index %s", indexPath)
	}
	tmp := indexPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(index.String()), 0o640); err != nil {
		return vterrors.Wrapf(err, "cannot write binlog index %s", tmp)
	}
	return vterrors.Wrapf(os.Rename(tmp, indexPath), "cannot install binlog index %s", indexPath)
}

// verifyRestoredBinlogs checks, without changing anything, that mysqld reads the
// binlogs the restore installed, that it can read each one, and that they put
// gtid_executed at the backup position. A mismatch fails the restore: repairing it
// with RESET would delete them.
func verifyRestoredBinlogs(ctx context.Context, mysqld MysqlDaemon, cnf *Mycnf, pos replication.Position) error {
	basename, indexPath, err := binlogLocation(cnf)
	if err != nil {
		return err
	}
	qr, err := mysqld.FetchSuperQuery(ctx, restoredBinlogVarsQuery)
	if err != nil {
		return vterrors.Wrap(err, "cannot read binlog location after restore")
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 2 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected result for %q: %v", restoredBinlogVarsQuery, qr.Rows)
	}
	if got := qr.Rows[0][0].ToString(); got != basename {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "restored binlogs are under %s but mysqld's log_bin_basename is %s", basename, got)
	}
	// mysqld uses log-bin-index as given but reports a custom extension as ".index",
	// so only the path without its extension is comparable.
	got := qr.Rows[0][1].ToString()
	if strings.TrimSuffix(got, filepath.Ext(got)) != strings.TrimSuffix(indexPath, filepath.Ext(indexPath)) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "restored binlog index is %s but mysqld's log_bin_index is %s", indexPath, got)
	}
	// An encrypted binlog is readable only with the binlog master key that encrypted
	// it, so a target whose keyring lacks the source's key can't serve it.
	qr, err = mysqld.FetchSuperQuery(ctx, showBinaryLogsQuery)
	if err != nil {
		return vterrors.Wrap(err, "cannot list binlogs after restore")
	}
	for _, row := range qr.Rows {
		name := row[0].ToString()
		if _, err := mysqld.FetchSuperQuery(ctx, "SHOW BINLOG EVENTS IN "+sqltypes.EncodeStringSQL(name)+" LIMIT 1"); err != nil {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot read restored binlog %s; if it is encrypted, this server's keyring needs the source's binlog master key: %v", name, err)
		}
	}
	executed, err := mysqld.PrimaryPosition(ctx)
	if err != nil {
		return vterrors.Wrap(err, "cannot read gtid_executed after restore")
	}
	if !executed.Equal(pos) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "gtid_executed after restoring binlogs is %v, expected the backup position %v", executed, pos)
	}
	return nil
}
