package vtgate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

//var ctx = context.Background()

type fakeSchema struct {
	t []SchemaTable
}

func (f fakeSchema) Tables(_ string) []SchemaTable {
	return f.t
}

func (f fakeSchema) ColumnsFor(table string) []vindexes.Column {
	panic("implement me")
}

func TestWatchSrvVSchema(t *testing.T) {
	cols := []vindexes.Column{{
		Name: sqlparser.NewColIdent("id"),
		Type: querypb.Type_INT64,
	}}

	vm := &VSchemaManager{
		schema: fakeSchema{t: []SchemaTable{{Name: "tbl", Columns: cols}}},
	}
	called := false
	vm.subscriber = func(vschema *vindexes.VSchema, _ *VSchemaStats) {
		called = true
		assert.Len(t, vschema.Keyspaces, 1)
		ks := vschema.Keyspaces["ks"]
		require.NotNil(t, ks, "keyspace was not found")
		require.NotNil(t, ks.Tables["tbl"])
		assert.Len(t, ks.Tables["tbl"].Columns, 1)
	}

	v := &vschemapb.SrvVSchema{Keyspaces: map[string]*vschemapb.Keyspace{"ks": {}}}
	vm.VSchemaUpdate(v, nil)
	assert.True(t, called, "assertions were never called")
}
