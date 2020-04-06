/*
Copyright 2019 The Vitess Authors.

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

package tabletserver

import (
	"errors"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/dbconfigs"
)

var errRejected = errors.New("rejected")

func newDBConfigs(db *fakesqldb.DB) *dbconfigs.DBConfigs {
	params, _ := db.ConnParams().MysqlParams()
	cp := *params
	return dbconfigs.NewTestDBConfigs(cp, cp, "")
}
