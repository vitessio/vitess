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

package vstreamer

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// localVSchema provides vschema behavior specific to vstreamer.
// Tables are searched within keyspace, but vindexes can be referenced
// outside the current keyspace.
type localVSchema struct {
	keyspace string
	vschema  *vindexes.VSchema
}

func (lvs *localVSchema) FindColVindex(tablename string) (*vindexes.ColumnVindex, error) {
	table, err := lvs.findTable(tablename)
	if err != nil {
		return nil, err
	}
	return vindexes.FindBestColVindex(table)
}

func (lvs *localVSchema) FindOrCreateVindex(qualifiedName string) (vindexes.Vindex, error) {
	splits := strings.Split(qualifiedName, ".")
	var keyspace, name string
	switch len(splits) {
	case 1:
		name = splits[0]
	case 2:
		keyspace, name = splits[0], splits[1]
	default:
		return nil, fmt.Errorf("invalid vindex name: %v", qualifiedName)
	}
	vindex, err := lvs.vschema.FindVindex(keyspace, name)
	if err != nil {
		return nil, err
	}
	if vindex != nil {
		return vindex, nil
	}
	if keyspace != "" {
		return nil, fmt.Errorf("vindex %v not found", qualifiedName)
	}
	return vindexes.CreateVindex(name, name, map[string]string{})
}

func (lvs *localVSchema) findTable(tablename string) (*vindexes.Table, error) {
	ks, ok := lvs.vschema.Keyspaces[lvs.keyspace]
	if !ok {
		return nil, fmt.Errorf("keyspace %s not found in vschema", lvs.keyspace)
	}
	table := ks.Tables[tablename]
	if table == nil {
		return nil, fmt.Errorf("table %s not found", tablename)
	}
	return table, nil
}
