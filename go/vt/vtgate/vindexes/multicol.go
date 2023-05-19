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

package vindexes

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	_ MultiColumn = (*MultiCol)(nil)
)

type MultiCol struct {
	name        string
	cost        int
	noOfCols    int
	columnVdx   map[int]Hashing
	columnBytes map[int]int
}

const (
	paramColumnCount  = "column_count"
	paramColumnBytes  = "column_bytes"
	paramColumnVindex = "column_vindex"
	defaultVindex     = "hash"
)

// newMultiCol creates a new MultiCol.
func newMultiCol(name string, m map[string]string) (Vindex, error) {
	colCount, err := getColumnCount(m)
	if err != nil {
		return nil, err
	}
	columnBytes, err := getColumnBytes(m, colCount)
	if err != nil {
		return nil, err
	}
	columnVdx, vindexCost, err := getColumnVindex(m, colCount)
	if err != nil {
		return nil, err
	}

	return &MultiCol{
		name:        name,
		cost:        vindexCost,
		noOfCols:    colCount,
		columnVdx:   columnVdx,
		columnBytes: columnBytes,
	}, nil
}

func (m *MultiCol) String() string {
	return m.name
}

func (m *MultiCol) Cost() int {
	return m.cost
}

func (m *MultiCol) IsUnique() bool {
	return true
}

func (m *MultiCol) NeedsVCursor() bool {
	return false
}

func (m *MultiCol) Map(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(rowsColValues))
	for _, colValues := range rowsColValues {
		partial, ksid, err := m.mapKsid(colValues)
		if err != nil {
			out = append(out, key.DestinationNone{})
			continue
		}
		if partial {
			out = append(out, NewKeyRangeFromPrefix(ksid))
			continue
		}
		out = append(out, key.DestinationKeyspaceID(ksid))
	}
	return out, nil
}

func (m *MultiCol) Verify(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, 0, len(rowsColValues))
	for idx, colValues := range rowsColValues {
		_, ksid, err := m.mapKsid(colValues)
		if err != nil {
			return nil, err
		}
		out = append(out, bytes.Equal(ksid, ksids[idx]))
	}
	return out, nil
}

func (m *MultiCol) PartialVindex() bool {
	return true
}

func (m *MultiCol) mapKsid(colValues []sqltypes.Value) (bool, []byte, error) {
	if m.noOfCols < len(colValues) {
		// wrong number of column values were passed
		return false, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] wrong number of column values were passed: maximum allowed %d, got %d", m.noOfCols, len(colValues))
	}
	ksid := make([]byte, 0, 8)
	minLength := 0
	for idx, colVal := range colValues {
		lksid, err := m.columnVdx[idx].Hash(colVal)
		if err != nil {
			return false, nil, err
		}
		// keyspace id should fill the minimum length i.e. the bytes utilized before the current column hash.
		padZero := minLength - len(ksid)
		for ; padZero > 0; padZero-- {
			ksid = append(ksid, uint8(0))
		}
		maxIndex := m.columnBytes[idx]
		for r, v := range lksid {
			if r >= maxIndex {
				break
			}
			ksid = append(ksid, v)
		}
		minLength = minLength + maxIndex
	}
	partial := m.noOfCols > len(colValues)
	return partial, ksid, nil
}

func init() {
	Register("multicol", newMultiCol)
}

func getColumnVindex(m map[string]string, colCount int) (map[int]Hashing, int, error) {
	var colVdxs []string
	colVdxsStr, ok := m[paramColumnVindex]
	if ok {
		colVdxs = strings.Split(colVdxsStr, ",")
	}
	if len(colVdxs) > colCount {
		return nil, 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of vindex function provided are more than column count in the parameter '%s'", paramColumnVindex)
	}
	columnVdx := make(map[int]Hashing, colCount)
	vindexCost := 0
	subParams := make(map[string]string)
	for k, v := range m {
		if k == paramColumnCount ||
			k == paramColumnBytes ||
			k == paramColumnVindex {
			continue
		}
		subParams[k] = v
	}
	for i := 0; i < colCount; i++ {
		selVdx := defaultVindex
		if len(colVdxs) > i {
			providedVdx := strings.TrimSpace(colVdxs[i])
			if providedVdx != "" {
				selVdx = providedVdx
			}
		}
		// TODO: reuse vindex. avoid creating same vindex.
		vdx, err := CreateVindex(selVdx, selVdx, subParams)
		if err != nil {
			return nil, 0, err
		}
		hashVdx, isHashVdx := vdx.(Hashing)
		if !isHashVdx || !vdx.IsUnique() || vdx.NeedsVCursor() {
			return nil, 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "multicol vindex supports vindexes that exports hashing function, are unique and are non-lookup vindex, passed vindex '%s' is invalid", selVdx)
		}
		vindexCost = vindexCost + vdx.Cost()
		columnVdx[i] = hashVdx
	}
	return columnVdx, vindexCost, nil
}

func getColumnBytes(m map[string]string, colCount int) (map[int]int, error) {
	var colByteStr []string
	colBytesStr, ok := m[paramColumnBytes]
	if ok {
		colByteStr = strings.Split(colBytesStr, ",")
	}
	if len(colByteStr) > colCount {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of column bytes provided are more than column count in the parameter '%s'", paramColumnBytes)
	}
	// validate bytes count
	bytesUsed := 0
	columnBytes := make(map[int]int, colCount)
	for idx, byteStr := range colByteStr {
		if byteStr == "" {
			continue
		}
		colByte, err := strconv.Atoi(byteStr)
		if err != nil {
			return nil, err
		}
		bytesUsed = bytesUsed + colByte
		columnBytes[idx] = colByte
	}
	pendingCol := colCount - len(columnBytes)
	remainingBytes := 8 - bytesUsed
	if pendingCol > remainingBytes {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "column bytes count exceeds the keyspace id length (total bytes count cannot exceed 8 bytes) in the parameter '%s'", paramColumnBytes)
	}
	if pendingCol <= 0 {
		return columnBytes, nil
	}
	for idx := 0; idx < colCount; idx++ {
		if _, defined := columnBytes[idx]; defined {
			continue
		}
		bytesToAssign := int(math.Ceil(float64(remainingBytes) / float64(pendingCol)))
		columnBytes[idx] = bytesToAssign
		remainingBytes = remainingBytes - bytesToAssign
		pendingCol--
	}
	return columnBytes, nil
}

func getColumnCount(m map[string]string) (int, error) {
	colCountStr, ok := m[paramColumnCount]
	if !ok {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of columns not provided in the parameter '%s'", paramColumnCount)
	}
	colCount, err := strconv.Atoi(colCountStr)
	if err != nil {
		return 0, err
	}
	if colCount > 8 || colCount < 1 {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of columns should be between 1 and 8 in the parameter '%s'", paramColumnCount)
	}
	return colCount, nil
}
