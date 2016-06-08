// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/schema"
)

func TestSchamazHandler(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/schemaz", nil)
	tableA := schema.NewTable("a")
	tableB := schema.NewTable("b")
	tableC := schema.NewTable("c")

	tableA.AddColumn("column1", sqltypes.Int64, sqltypes.MakeTrusted(sqltypes.Int32, []byte("0")), "auto_increment")
	tableA.AddIndex("index1").AddColumn("index_column", 1000)
	tableA.Type = schema.NoType

	tableB.AddColumn("column2", sqltypes.VarChar, sqltypes.MakeString([]byte("NULL")), "")
	tableB.AddIndex("index2").AddColumn("index_column2", 200)
	tableB.Type = schema.Sequence

	tables := []*schema.Table{
		tableA, tableB, tableC,
	}
	schemazHandler(tables, resp, req)
	body, _ := ioutil.ReadAll(resp.Body)
	tableBPattern := []string{
		`<td>b</td>`,
		`<td>column2: VARCHAR, , NULL<br></td>`,
		`<td>index2: \(index_column2,\), \(200,\)<br></td>`,
		`<td>sequence</td>`,
	}
	matched, err := regexp.Match(strings.Join(tableBPattern, `\s*`), body)
	if err != nil {
		t.Fatalf("schemaz page does not contain table B with error: %v", err)
	}
	if !matched {
		t.Fatalf("schemaz page does not contain table B")
	}
	tableAPattern := []string{
		`<td>a</td>`,
		`<td>column1: INT64, autoinc, <br></td>`,
		`<td>index1: \(index_column,\), \(1000,\)<br></td>`,
		`<td>none</td>`,
	}
	matched, err = regexp.Match(strings.Join(tableAPattern, `\s*`), body)
	if err != nil {
		t.Fatalf("schemaz page does not contain table A with error: %v", err)
	}
	if !matched {
		t.Fatalf("schemaz page does not contain table A")
	}
}
