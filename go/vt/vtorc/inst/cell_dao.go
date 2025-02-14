/*
Copyright 2025 The Vitess Authors.

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

package inst

import (
	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/vtorc/db"
)

// ReadCells reads all the vitess cell names.
func ReadCells() ([]string, error) {
	cells := make([]string, 0)
	query := `SELECT cell FROM vitess_cell`
	err := db.QueryVTOrc(query, nil, func(row sqlutils.RowMap) error {
		cells = append(cells, row.GetString("cell"))
		return nil
	})
	return cells, err
}

// SaveCell saves the keyspace record against the keyspace name.
func SaveCell(cell string) error {
	_, err := db.ExecVTOrc(`REPLACE INTO vitess_cell (cell) VALUES(?)`, cell)
	return err
}

// DeleteCell deletes a cell.
func DeleteCell(cell string) (err error) {
	_, err = db.ExecVTOrc(`DELETE FROM vitess_cell WHERE cell = ?`, cell)
	return err
}
