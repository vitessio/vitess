/*
Copyright 2026 The Vitess Authors.

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

package primaryfailure

import (
	"encoding/json"
	"fmt"
	"net/http"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/external/golib/sqlutils"
)

// vtorcTable is one table in the response of the VTOrc database-state API.
type vtorcTable struct {
	TableName string
	Rows      []sqlutils.RowMap
}

// readVTOrcTable returns the rows of one VTOrc table from the database-state API.
func readVTOrcTable(vtorc *cluster.VTOrcProcess, name string) ([]sqlutils.RowMap, error) {
	status, body, err := vtorc.MakeAPICall("api/database-state")
	if err != nil {
		return nil, err
	}

	if status != http.StatusOK {
		return nil, fmt.Errorf("database-state returned HTTP %d: %s", status, body)
	}

	var tables []vtorcTable
	if err := json.Unmarshal([]byte(body), &tables); err != nil {
		return nil, err
	}

	for _, table := range tables {
		if table.TableName == name {
			return table.Rows, nil
		}
	}

	return nil, fmt.Errorf("VTOrc table %s is missing", name)
}
