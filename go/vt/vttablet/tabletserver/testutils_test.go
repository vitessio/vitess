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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/dbconfigs"
)

var errRejected = errors.New("rejected")

func newDBConfigs(db *fakesqldb.DB) *dbconfigs.DBConfigs {
	params := db.ConnParams()
	cp := *params
	return dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
}

// requireLogs ensure that the given logs contains all the string in wants.
// the input logs string must be a semicolon separated string.
func requireLogs(t *testing.T, logs string, wants ...string) {
	logLines := strings.Split(logs, ";")
	for _, expectedLogLine := range wants {
		require.Contains(t, logLines, expectedLogLine)
	}
}
