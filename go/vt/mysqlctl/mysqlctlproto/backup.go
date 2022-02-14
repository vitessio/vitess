package mysqlctlproto

import (
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"

	mysqlctlpb "vitess.io/vitess/go/vt/proto/mysqlctl"
)

// BackupHandleToProto returns a BackupInfo proto from a BackupHandle.
func BackupHandleToProto(bh backupstorage.BackupHandle) *mysqlctlpb.BackupInfo {
	bi := &mysqlctlpb.BackupInfo{
		Name:      bh.Name(),
		Directory: bh.Directory(),
	}

	btime, alias, err := mysqlctl.ParseBackupName(bi.Directory, bi.Name)
	if err != nil { // if bi.Name does not match expected format, don't parse any further fields
		return bi
	}

	if btime != nil {
		bi.Time = protoutil.TimeToProto(*btime)
	}

	bi.TabletAlias = alias

	return bi
}
