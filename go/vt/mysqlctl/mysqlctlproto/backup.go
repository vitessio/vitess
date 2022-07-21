/*
Copyright 2021 The Vitess Authors.

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
