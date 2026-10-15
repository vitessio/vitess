package mysqlctl

import (
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
