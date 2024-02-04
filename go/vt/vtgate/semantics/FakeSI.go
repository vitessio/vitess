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

package semantics

import (
	"fmt"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ SchemaInformation = (*FakeSI)(nil)

// FakeSI is a fake SchemaInformation for testing
type FakeSI struct {
	Tables           map[string]*vindexes.Table
	VindexTables     map[string]vindexes.Vindex
	KsForeignKeyMode map[string]vschemapb.Keyspace_ForeignKeyMode
	KsError          map[string]error
}

// FindTableOrVindex implements the SchemaInformation interface
func (s *FakeSI) FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error) {
	table, ok := s.Tables[sqlparser.String(tablename)]
	if ok {
		return table, nil, "", 0, nil, nil
	}
	return nil, s.VindexTables[sqlparser.String(tablename)], "", 0, nil, nil
}

func (*FakeSI) ConnCollation() collations.ID {
	return collations.CollationUtf8mb4ID
}

func (s *FakeSI) Environment() *vtenv.Environment {
	return vtenv.NewTestEnv()
}

func (s *FakeSI) ForeignKeyMode(keyspace string) (vschemapb.Keyspace_ForeignKeyMode, error) {
	if s.KsForeignKeyMode != nil {
		fkMode, isPresent := s.KsForeignKeyMode[keyspace]
		if !isPresent {
			return vschemapb.Keyspace_unspecified, fmt.Errorf("%v keyspace not found", keyspace)
		}
		return fkMode, nil
	}
	return vschemapb.Keyspace_unmanaged, nil
}

func (s *FakeSI) GetForeignKeyChecksState() *bool {
	return nil
}

func (s *FakeSI) KeyspaceError(keyspace string) error {
	if s.KsError != nil {
		fkErr, isPresent := s.KsError[keyspace]
		if !isPresent {
			return fmt.Errorf("%v keyspace not found", keyspace)
		}
		return fkErr
	}
	return nil
}
