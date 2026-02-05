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

package backupstorage

import (
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
)

// Params contains common utilities that may be used by BackupStorage and FileHandle components.
// This gives out-of-three backup/restore plugins mechanisms to log and generate metrics,
// while keeping policies (metric names and labels, logging destination, etc.)
// in the control of in-tree components and Vitess users.
type Params struct {
	Logger logutil.Logger
	Stats  backupstats.Stats
}

// NoParams gives BackupStorage components way to log and generate stats
// without doing nil checking.
func NoParams() Params {
	return Params{
		Logger: logutil.NewCallbackLogger(func(*logutilpb.Event) {}),
		Stats:  backupstats.NoStats(),
	}
}
