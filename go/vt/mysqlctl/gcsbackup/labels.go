package gcsbackup

import (
	"bufio"
	"os"
	"strings"

	"vitess.io/vitess/go/vt/vterrors"
)

const (
	lastBackupIDLabel                = "psdb.co/last-backup-id"
	lastBackupExcludedKeyspacesLabel = "psdb.co/last-backup-excluded-keyspaces"
	backupIDLabel                    = "psdb.co/backup-id"
)

// Labels represents loaded labels.
//
// Labels are set by Singularity and are meant to tie
// state such as last backup to the backup system.
type labels struct {
	LastBackupID                string
	LastBackupExcludedKeyspaces []string
	BackupID                    string
}

// Load returns all labels from the given file path.
func load(path string) (*labels, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, vterrors.Wrap(err, "open annotations file")
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	r := labels{}

	for s.Scan() {
		parts := strings.SplitN(s.Text(), "=", 2)

		if len(parts) != 2 {
			continue
		}

		switch parts[0] {
		case lastBackupIDLabel:
			r.LastBackupID = strings.TrimSpace(parts[1])
		case lastBackupExcludedKeyspacesLabel:
			r.LastBackupExcludedKeyspaces = split(parts[1])
		case backupIDLabel:
			r.BackupID = strings.TrimSpace(parts[1])
		}
	}

	return &r, nil
}

// Split splits and trims the given str by comma.
//
// It ensures there are no empty strings in the returned
// slice by trimming each part, if the part is empty it is
// discarded.
//
// Usage:
//
//   split("a,, b,c ") // => {"a", "b", "c"}
//
func split(str string) (ret []string) {
	for _, part := range strings.Split(str, ",") {
		if part := strings.TrimSpace(part); part != "" {
			ret = append(ret, part)
		}
	}
	return
}
