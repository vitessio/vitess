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

// mariaDB10 is the implementation of MysqlFlavor for MariaDB 10.0.10
type mariaDB10 struct {
}

const mariadbFlavorID = "MariaDB"

// VersionMatch implements MysqlFlavor.VersionMatch().
func (*mariaDB10) VersionMatch(version string) bool {
	return strings.HasPrefix(version, "10.0") && strings.Contains(strings.ToLower(version), "mariadb")
}

// ParseGTID implements MysqlFlavor.ParseGTID().
func (*mariaDB10) ParseGTID(s string) (mysql.GTID, error) {
	return mysql.ParseGTID(mariadbFlavorID, s)
}

// EnableBinlogPlayback implements MysqlFlavor.EnableBinlogPlayback().
func (*mariaDB10) EnableBinlogPlayback(mysqld *Mysqld) error {
	return nil
}

// DisableBinlogPlayback implements MysqlFlavor.DisableBinlogPlayback().
func (*mariaDB10) DisableBinlogPlayback(mysqld *Mysqld) error {
	return nil
}

func init() {
	registerFlavorBuiltin(mariadbFlavorID, &mariaDB10{})
}
