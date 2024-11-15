/*
Copyright 2024 The Vitess Authors.

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

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmp "vitess.io/vitess/go/test/utils"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestParseResult(t *testing.T) {
	packets := [][]byte{
		{1},
		{3, 100, 101, 102, 0, 6, 84, 65, 66, 76, 69, 83, 0, 15, 84, 97, 98, 108, 101, 115, 95, 105, 110, 95, 118, 116, 95, 107, 115, 15, 84, 97, 98, 108, 101, 115, 95, 105, 110, 95, 118, 116, 95, 107, 115, 12, 255, 0, 0, 1, 0, 0, 253, 0, 0, 0, 0, 0},
		{2, 116, 49},
		{3, 116, 49, 48},
		{25, 116, 49, 48, 95, 105, 100, 95, 116, 111, 95, 107, 101, 121, 115, 112, 97, 99, 101, 95, 105, 100, 95, 105, 100, 120},
		{3, 116, 49, 49},
		{10, 116, 49, 95, 105, 100, 50, 95, 105, 100, 120},
		{2, 116, 50},
		{10, 116, 50, 95, 105, 100, 52, 95, 105, 100, 120},
		{2, 116, 51},
		{10, 116, 51, 95, 105, 100, 55, 95, 105, 100, 120},
		{2, 116, 52},
		{10, 116, 52, 95, 105, 100, 50, 95, 105, 100, 120},
		{14, 116, 53, 95, 110, 117, 108, 108, 95, 118, 105, 110, 100, 101, 120},
		{2, 116, 54},
		{10, 116, 54, 95, 105, 100, 50, 95, 105, 100, 120},
		{5, 116, 55, 95, 102, 107},
		{9, 116, 55, 95, 120, 120, 104, 97, 115, 104},
		{13, 116, 55, 95, 120, 120, 104, 97, 115, 104, 95, 105, 100, 120},
		{2, 116, 56},
		{2, 116, 57},
		{24, 116, 57, 95, 105, 100, 95, 116, 111, 95, 107, 101, 121, 115, 112, 97, 99, 101, 95, 105, 100, 95, 105, 100, 120},
		{12, 118, 115, 116, 114, 101, 97, 109, 95, 116, 101, 115, 116},
		{254, 0, 0, 2, 0, 0, 0},
	}
	er := &querypb.ExecuteResponse{RawPackets: packets}
	qr, err := ParseResult(er, true)
	require.NoError(t, err)

	// validations
	assert.EqualValues(t, 1, len(qr.Fields))
	exp := &querypb.Field{Name: "Tables_in_vt_ks", Type: querypb.Type_VARCHAR, Table: "TABLES", OrgName: "Tables_in_vt_ks", ColumnLength: 256, Charset: 255}
	cmp.MustMatch(t, exp, qr.Fields[0])
	assert.EqualValues(t, 21, len(qr.Rows))
	assert.EqualValues(t, 0, qr.RowsAffected)
	assert.EqualValues(t, 0, qr.InsertID)
	assert.EqualValues(t, 0x2, qr.StatusFlags)
	assert.EqualValues(t, ``, qr.SessionStateChanges)
	assert.EqualValues(t, ``, qr.Info)
}
