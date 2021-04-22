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
// +build gofuzz

package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ semantics.SchemaInformation = (*fakeFuzzSI)(nil)

type fakeFuzzSI struct {
	tables map[string]*vindexes.Table
}

// Helper func:
func (s *fakeFuzzSI) FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error) {
	return s.tables[sqlparser.String(tablename)], nil, "", 0, nil, nil
}

// FuzzAnalyse implements the fuzzer
func FuzzAnalyse(data []byte) int {
	tree, err := sqlparser.Parse(string(data))
	if err != nil {
		return -1
	}
	semTable, err := semantics.Analyse(tree, &fakeFuzzSI{})
	if err != nil {
		return 0
	}
	_, _ = createQGFromSelect(tree.(*sqlparser.Select), semTable)
	return 1
}
