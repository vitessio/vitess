/*
Copyright 2021 The Vitess Authors.

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

package test

import (
	"fmt"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/external/golib/sqlutils"
)

var _ db.DB = (*DB)(nil)

type DB struct {
	rowMaps [][]sqlutils.RowMap
}

func NewTestDB(rowMaps [][]sqlutils.RowMap) *DB {
	return &DB{
		rowMaps: rowMaps,
	}
}

func (t *DB) QueryVTOrc(query string, argsArray []any, onRow func(sqlutils.RowMap) error) error {
	log.Info("test")
	rowMaps, err := t.getRowMapsForQuery()
	if err != nil {
		return err
	}
	for _, rowMap := range rowMaps {
		err = onRow(rowMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *DB) getRowMapsForQuery() ([]sqlutils.RowMap, error) {
	if len(t.rowMaps) == 0 {
		return nil, fmt.Errorf("no rows left to return. We received more queries than expected")
	}
	result := t.rowMaps[0]
	t.rowMaps = t.rowMaps[1:]
	return result, nil
}
