/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vindexes

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// multiColLookupInternal implements the functions for the Lookup vindexes.
type multiColLookupInternal struct {
	Table         string   `json:"table"`
	FromColumns   []string `json:"ordered_from"`
	To            string   `json:"to"`
	sel, ver, del string
}

func (lkp *multiColLookupInternal) Init(lookupQueryParams map[string]string) {
	lkp.Table = lookupQueryParams["table"]
	lkp.To = lookupQueryParams["to"]
	var fromColumns []string
	for _, from := range strings.Split(lookupQueryParams["from_columns"], ",") {
		fromColumns = append(fromColumns, strings.TrimSpace(from))
	}
	lkp.FromColumns = fromColumns

	// TODO
	lkp.sel = fmt.Sprintf("select %s from %s where %s = :%s", lkp.To, lkp.Table, lkp.FromColumns, lkp.FromColumns)
	lkp.ver = fmt.Sprintf("select %s from %s where %s = :%s and %s = :%s", lkp.FromColumns, lkp.Table, lkp.FromColumns, lkp.FromColumns, lkp.To, lkp.To)
	lkp.del = fmt.Sprintf("delete from %s where %s = :%s and %s = :%s", lkp.Table, lkp.FromColumns, lkp.FromColumns, lkp.To, lkp.To)
}

// Lookup performs a lookup for the ids.
func (lkp *multiColLookupInternal) Lookup(vcursor VCursor, ids []sqltypes.Value) ([]*sqltypes.Result, error) {
	results := make([]*sqltypes.Result, 0, len(ids))
	for _, id := range ids {
		bindVars := map[string]*querypb.BindVariable{
			lkp.FromColumns[0]: sqltypes.ValueBindVariable(id),
		}
		result, err := vcursor.Execute(lkp.sel, bindVars, false /* isDML */)
		if err != nil {
			return nil, fmt.Errorf("lookup.Map: %v", err)
		}
		results = append(results, result)
	}
	return results, nil
}

// Verify returns true if ids map to values.
func (lkp *multiColLookupInternal) Verify(vcursor VCursor, ids, values []sqltypes.Value) ([]bool, error) {
	out := make([]bool, len(ids))
	for i, id := range ids {
		bindVars := map[string]*querypb.BindVariable{
			// TODO think
			lkp.FromColumns[0]: sqltypes.ValueBindVariable(id),
			lkp.To:             sqltypes.ValueBindVariable(values[i]),
		}
		result, err := vcursor.Execute(lkp.ver, bindVars, true /* isDML */)
		if err != nil {
			return nil, fmt.Errorf("lookup.Verify: %v", err)
		}
		out[i] = (len(result.Rows) != 0)
	}
	return out, nil
}

// Create creates an association between fromIds and toValues by inserting rows in the vindex table.
func (lkp *multiColLookupInternal) Create(vcursor VCursor, fromIds [][]sqltypes.Value, toValues []sqltypes.Value, ignoreMode bool) error {
	var insBuffer bytes.Buffer
	if ignoreMode {
		fmt.Fprintf(&insBuffer, "insert ignore into %s(", lkp.Table)
	} else {
		fmt.Fprintf(&insBuffer, "insert into %s(", lkp.Table)
	}
	for _, col := range lkp.FromColumns {
		fmt.Fprintf(&insBuffer, "%s, ", col)

	}

	fmt.Fprintf(&insBuffer, "%s) values(", lkp.To)
	bindVars := make(map[string]*querypb.BindVariable, 2*len(fromIds))
	for rowIdx, _ := range toValues {
		colIds := fromIds[rowIdx]
		if rowIdx != 0 {
			insBuffer.WriteString(", (")
		}
		for colIdx, colId := range colIds {
			fromStr := lkp.FromColumns[colIdx] + strconv.Itoa(rowIdx)
			bindVars[fromStr] = sqltypes.ValueBindVariable(colId)
			insBuffer.WriteString(":" + fromStr + ", ")
		}
		toStr := lkp.To + strconv.Itoa(rowIdx)
		insBuffer.WriteString(":" + toStr + ")")
		bindVars[toStr] = sqltypes.ValueBindVariable(toValues[rowIdx])
	}
	_, err := vcursor.Execute(insBuffer.String(), bindVars, true /* isDML */)
	if err != nil {
		return fmt.Errorf("lookup.Create: %v", err)
	}
	return err
}

// Delete deletes the association between ids and value.
func (lkp *multiColLookupInternal) Delete(vcursor VCursor, ids []sqltypes.Value, value sqltypes.Value) error {
	for _, id := range ids {
		bindVars := map[string]*querypb.BindVariable{
			lkp.FromColumns[0]: sqltypes.ValueBindVariable(id),
			lkp.To:             sqltypes.ValueBindVariable(value),
		}
		if _, err := vcursor.Execute(lkp.del, bindVars, true /* isDML */); err != nil {
			return fmt.Errorf("lookup.Delete: %v", err)
		}
	}
	return nil
}
