// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package mysqlctl

import (
	"fmt"
)

/*
The code in this file helps to track the position metadata
for binlogs.
*/

type ReplicationCoordinates struct {
	MasterFilename string
	MasterPosition uint64
}

func NewReplicationCoordinates(masterFile string, masterPos uint64) *ReplicationCoordinates {
	return &ReplicationCoordinates{
		MasterFilename: masterFile,
		MasterPosition: masterPos,
	}
}

func (repl *ReplicationCoordinates) String() string {
	return fmt.Sprintf("Master %v:%v", repl.MasterFilename, repl.MasterPosition)
}
