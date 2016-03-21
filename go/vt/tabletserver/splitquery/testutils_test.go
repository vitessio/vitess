package splitquery

// This file contains utility routines for used in splitquery tests.

import (
	"strconv"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/schema"
)

// GetSchema returns a fake schema object that can be given to SplitParams
func GetSchema() map[string]*schema.Table {
	table := schema.Table{
		Name: "test_table",
	}
	zero, _ := sqltypes.BuildValue(0)
	table.AddColumn("id", sqltypes.Int64, zero, "")
	table.AddColumn("int32_col", sqltypes.Int32, zero, "")
	table.AddColumn("uint32_col", sqltypes.Uint32, zero, "")
	table.AddColumn("int64_col", sqltypes.Int64, zero, "")
	table.AddColumn("uint64_col", sqltypes.Uint64, zero, "")
	table.AddColumn("float32_col", sqltypes.Float32, zero, "")
	table.AddColumn("float64_col", sqltypes.Float64, zero, "")
	table.AddColumn("user_id", sqltypes.Int64, zero, "")
	table.AddColumn("user_id2", sqltypes.Int64, zero, "")
	table.AddColumn("id2", sqltypes.Int64, zero, "")
	table.AddColumn("count", sqltypes.Int64, zero, "")
	table.PKColumns = []int{0}
	primaryIndex := table.AddIndex("PRIMARY")
	primaryIndex.AddColumn("id", 12345)

	id2Index := table.AddIndex("idx_id2")
	id2Index.AddColumn("id2", 1234)

	result := make(map[string]*schema.Table)
	result["test_table"] = &table

	tableNoPK := schema.Table{
		Name: "test_table_no_pk",
	}
	tableNoPK.AddColumn("id", sqltypes.Int64, zero, "")
	tableNoPK.PKColumns = []int{}
	result["test_table_no_pk"] = &tableNoPK

	return result
}

// Int64Value builds a sqltypes.Value of type sqltypes.Int64 containing the given int64 value.
func Int64Value(value int64) sqltypes.Value {
	return sqltypes.MakeTrusted(sqltypes.Int64, strconv.AppendInt([]byte{}, value, 10))
}

// Uint64Value builds a sqltypes.Value of type sqltypes.Uint64 containing the given uint64 value.
func Uint64Value(value uint64) sqltypes.Value {
	return sqltypes.MakeTrusted(sqltypes.Uint64, strconv.AppendUint([]byte{}, value, 10))
}

// Float64Value builds a sqltypes.Value of type sqltypes.Float64 containing the given float64 value.
func Float64Value(value float64) sqltypes.Value {
	return sqltypes.MakeTrusted(sqltypes.Float64, strconv.AppendFloat([]byte{}, value, 'f', -1, 64))
}
