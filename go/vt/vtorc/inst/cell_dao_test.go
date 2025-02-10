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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtorc/db"
)

func TestSaveReadAndDeleteCells(t *testing.T) {
	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()
	cells := []string{"zone1", "zone2", "zone3"}
	for _, cell := range cells {
		require.NoError(t, SaveCell(cell))
	}
	cellsRead, err := ReadCells()
	require.NoError(t, err)
	require.Equal(t, cells, cellsRead)

	require.NoError(t, DeleteCell("zone3"))
	cellsRead, err = ReadCells()
	require.NoError(t, err)
	require.Equal(t, []string{"zone1", "zone2"}, cellsRead)
}
