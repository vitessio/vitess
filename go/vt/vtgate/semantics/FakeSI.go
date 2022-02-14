package semantics

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ SchemaInformation = (*FakeSI)(nil)

// FakeSI is a fake SchemaInformation for testing
type FakeSI struct {
	Tables       map[string]*vindexes.Table
	VindexTables map[string]vindexes.Vindex
}

// FindTableOrVindex implements the SchemaInformation interface
func (s *FakeSI) FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error) {
	table, ok := s.Tables[sqlparser.String(tablename)]
	if ok {
		return table, nil, "", 0, nil, nil
	}
	return nil, s.VindexTables[sqlparser.String(tablename)], "", 0, nil, nil
}

func (FakeSI) ConnCollation() collations.ID {
	return 45
}
