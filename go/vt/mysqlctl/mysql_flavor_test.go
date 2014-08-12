// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"os"
	"testing"

	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

type fakeMysqlFlavor struct{}

func (fakeMysqlFlavor) MasterStatus(mysqld *Mysqld) (*proto.ReplicationPosition, error) {
	return nil, nil
}
func (fakeMysqlFlavor) PromoteSlaveCommands() []string       { return nil }
func (fakeMysqlFlavor) ParseGTID(string) (proto.GTID, error) { return nil, nil }
func (fakeMysqlFlavor) SendBinlogDumpCommand(mysqld *Mysqld, conn *SlaveConnection, startPos proto.GTID) error {
	return nil
}
func (fakeMysqlFlavor) MakeBinlogEvent(buf []byte) blproto.BinlogEvent {
	return nil
}

func TestDefaultMysqlFlavor(t *testing.T) {
	os.Setenv("MYSQL_FLAVOR", "")
	mysqlFlavors = make(map[string]MysqlFlavor)
	mysqlFlavors["only one"] = &fakeMysqlFlavor{}
	want := mysqlFlavors["only one"]

	if got := mysqlFlavor(); got != want {
		t.Errorf("mysqlFlavor() = %#v, want %#v", got, want)
	}
}

func TestMysqlFlavorEnvironmentVariable(t *testing.T) {
	os.Setenv("MYSQL_FLAVOR", "fake flavor")
	mysqlFlavors = make(map[string]MysqlFlavor)
	mysqlFlavors["fake flavor"] = &fakeMysqlFlavor{}
	want := mysqlFlavors["fake flavor"]

	if got := mysqlFlavor(); got != want {
		t.Errorf("mysqlFlavor() = %#v, want %#v", got, want)
	}
}
