package mysqlctl

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"

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

// binlogBackupVarsQuery reads the settings that decide whether the source's binlogs can be backed up.
const binlogBackupVarsQuery = "SELECT @@global.log_bin, @@global.binlog_encryption, @@global.log_replica_updates"

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
// transaction in the data (no binlog, or a replica without log_replica_updates), or
// that another server could not read (binlog encryption is keyed to this server).
func checkBinlogsBackupable(ctx context.Context, mysqld MysqlDaemon, sourceIsPrimary bool) error {
	qr, err := mysqld.FetchSuperQuery(ctx, binlogBackupVarsQuery)
	if err != nil {
		return vterrors.Wrap(err, "cannot read binlog settings")
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 3 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected result for %q: %v", binlogBackupVarsQuery, qr.Rows)
	}
	row := qr.Rows[0]
	switch {
	case row[0].ToString() != "1":
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot include binlogs: log_bin is off")
	case row[1].ToString() != "0":
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot include binlogs: binlog_encryption is on, so another server cannot read them")
	case !sourceIsPrimary && row[2].ToString() != "1":
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot include binlogs: log_replica_updates is off, so this replica's binlogs lack replicated transactions")
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
