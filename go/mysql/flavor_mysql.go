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

package mysql

import "fmt"

// mysqlFlavor implements the Flavor interface for Mysql.
type mysqlFlavor struct{}

// masterGTIDSet is part of the Flavor interface.
func (mysqlFlavor) masterGTIDSet(conn *Conn) (GTIDSet, error) {
	qr, err := conn.ExecuteFetch("SELECT @@GLOBAL.gtid_executed", 1, false)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return nil, fmt.Errorf("unexpected result format for gtid_executed: %#v", qr)
	}
	return parseMysql56GTIDSet(qr.Rows[0][0].String())
}
