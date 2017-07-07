/*
Copyright 2017 Google Inc.

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
	"strings"

	"github.com/youtube/vitess/go/mysql"
)

// mysql56 is the implementation of MysqlFlavor for MySQL 5.6+.
type mysql56 struct {
}

const mysql56FlavorID = "MySQL56"

// VersionMatch implements MysqlFlavor.VersionMatch().
func (*mysql56) VersionMatch(version string) bool {
	return strings.HasPrefix(version, "5.6") ||
		strings.HasPrefix(version, "5.7") ||
		strings.HasPrefix(version, "8.0")
}

// ParseGTID implements MysqlFlavor.ParseGTID().
func (*mysql56) ParseGTID(s string) (mysql.GTID, error) {
	return mysql.ParseGTID(mysql56FlavorID, s)
}

// MakeBinlogEvent implements MysqlFlavor.MakeBinlogEvent().
func (*mysql56) MakeBinlogEvent(buf []byte) mysql.BinlogEvent {
	return mysql.NewMysql56BinlogEvent(buf)
}

// EnableBinlogPlayback implements MysqlFlavor.EnableBinlogPlayback().
func (*mysql56) EnableBinlogPlayback(mysqld *Mysqld) error {
	return nil
}

// DisableBinlogPlayback implements MysqlFlavor.DisableBinlogPlayback().
func (*mysql56) DisableBinlogPlayback(mysqld *Mysqld) error {
	return nil
}

func init() {
	registerFlavorBuiltin(mysql56FlavorID, &mysql56{})
}
