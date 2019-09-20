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

package schema

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"vitess.io/vitess/go/sqltypes"
)

func TestSchamazHandler(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/schemaz", nil)
	tableA := NewTable("a")
	tableB := NewTable("b")
	tableC := NewTable("c")
	tableD := NewTable("c")

	tableA.AddColumn("column1", sqltypes.Int64, sqltypes.NewInt32(0), "auto_increment")
	tableA.AddIndex("index1", true).AddColumn("index_column", 1000)
	tableA.Type = NoType

	tableB.AddColumn("column2", sqltypes.VarChar, sqltypes.NewVarBinary("NULL"), "")
	tableB.AddIndex("index2", false).AddColumn("index_column2", 200)
	tableB.Type = Sequence

	tableC.Type = Message

	tables := map[string]*Table{
		"a": tableA,
		"b": tableB,
		"c": tableC,
		"d": tableD,
	}
	schemazHandler(tables, resp, req)
	body, _ := ioutil.ReadAll(resp.Body)
	tableAPattern := []string{
		`<td>a</td>`,
		`<td>column1: INT64, autoinc, <br></td>`,
		`<td>index1\(unique\): \(index_column,\), \(1000,\)<br></td>`,
		`<td>none</td>`,
	}
	matched, err := regexp.Match(strings.Join(tableAPattern, `\s*`), body)
	if err != nil {
		t.Fatalf("schemaz page does not contain table A with error: %v", err)
	}
	if !matched {
		t.Fatal("schemaz page does not contain table A")
	}
	tableBPattern := []string{
		`<td>b</td>`,
		`<td>column2: VARCHAR, , NULL<br></td>`,
		`<td>index2: \(index_column2,\), \(200,\)<br></td>`,
		`<td>sequence</td>`,
	}
	matched, err = regexp.Match(strings.Join(tableBPattern, `\s*`), body)
	if err != nil {
		t.Fatalf("schemaz page does not contain table B with error: %v", err)
	}
	if !matched {
		t.Fatalf("schemaz page does not contain table B")
	}
	tableCPattern := []string{
		`<td>c</td>`,
		`<td></td>`,
		`<td></td>`,
		`<td>message</td>`,
	}
	matched, err = regexp.Match(strings.Join(tableCPattern, `\s*`), body)
	if err != nil {
		t.Fatalf("schemaz page does not contain table B with error: %v", err)
	}
	if !matched {
		t.Fatalf("schemaz page does not contain table B")
	}
}
