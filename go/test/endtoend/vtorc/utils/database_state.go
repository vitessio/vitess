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

package utils

import (
	"encoding/json"
	"fmt"
	"net/http"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/external/golib/sqlutils"
)

// VTOrcTableState decodes a table from the database-state API.
type VTOrcTableState struct {
	// TableName identifies the stored table.
	TableName string
	// Rows contains the stored column values.
	Rows []sqlutils.RowMap
}

// ReadVTOrcTable reads stored rows without test assertions so callers can retry.
func ReadVTOrcTable(vtorc *cluster.VTOrcProcess, name string) ([]sqlutils.RowMap, error) {
	status, body, err := vtorc.MakeAPICall("api/database-state")
	if err != nil {
		return nil, err
	}

	if status != http.StatusOK {
		return nil, fmt.Errorf("database-state returned HTTP %d: %s", status, body)
	}

	var tables []VTOrcTableState
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
