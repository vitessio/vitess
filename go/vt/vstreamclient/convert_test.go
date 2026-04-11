/*
Copyright 2025 The Vitess Authors.

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

package vstreamclient

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

type testRow struct {
	ID   int64      `vstream:"id"`
	TS   time.Time  `vstream:"ts"`
	Opt  *int64     `vstream:"opt"`
	OptT *time.Time `vstream:"opt_ts"`
}

type testRowSmall struct {
	ID int64 `vstream:"id"`
}

type testScannerRow struct {
	ID int64
}

type testFailingScannerRow struct{}

type testJSONPayload struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type testJSONRow struct {
	ID         int64            `vstream:"id"`
	Payload    testJSONPayload  `vstream:"payload,json"`
	OptPayload *testJSONPayload `vstream:"opt_payload,json"`
}

type testFallbackTagRow struct {
	DBName     string `db:"db_name,omitempty"`
	JSONName   string `json:"json_name,omitempty"`
	OptionName string `vstream:" option_name , JSON "`
}

type testNullableJSONRow struct {
	ID      int64            `vstream:"id"`
	Opt     *int64           `vstream:"opt"`
	Payload *testJSONPayload `vstream:"payload,json"`
}

type testCustomStructField struct {
	Value string
}

type testStructFieldRow struct {
	ID      int64                 `vstream:"id"`
	Wrapped testCustomStructField `vstream:"wrapped"`
}

type testSmallIntRow struct {
	Value int8 `vstream:"value"`
}

type testSmallUintRow struct {
	Value uint8 `vstream:"value"`
}

type testSmallFloatRow struct {
	Value float32 `vstream:"value"`
}

type testBytesAndRawJSONRow struct {
	Payload    []byte           `vstream:"payload"`
	RawPayload json.RawMessage  `vstream:"raw_payload,json"`
	OptRaw     *json.RawMessage `vstream:"opt_raw,json"`
}

type TestEmbeddedFieldsExported struct {
	ID int64 `vstream:"id"`
}

type testNestedFields struct {
	Name string `vstream:"name"`
}

type testNestedMappedRow struct {
	TestEmbeddedFieldsExported
	Meta testNestedFields
}

type testTextWrapper string

type testWrapperRow struct {
	Name   sql.NullString  `vstream:"name"`
	Count  sql.NullInt64   `vstream:"count"`
	SeenAt sql.NullTime    `vstream:"seen_at"`
	Level  testTextWrapper `vstream:"level"`
}

func (w *testTextWrapper) UnmarshalText(text []byte) error {
	*w = testTextWrapper("wrapped:" + string(text))
	return nil
}

func (r *testScannerRow) VStreamScan(_ []*querypb.Field, row []sqltypes.Value, _ *binlogdatapb.RowEvent, _ *binlogdatapb.RowChange) error {
	v, err := row[0].ToInt64()
	if err != nil {
		return err
	}
	r.ID = v
	return nil
}

func (r *testFailingScannerRow) VStreamScan(_ []*querypb.Field, _ []sqltypes.Value, _ *binlogdatapb.RowEvent, _ *binlogdatapb.RowChange) error {
	return assert.AnError
}

func TestCopyRowToStruct_TimeAndPointers(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT64},
		{Name: "ts", Type: querypb.Type_TIMESTAMP},
		{Name: "opt", Type: querypb.Type_INT64},
		{Name: "opt_ts", Type: querypb.Type_TIMESTAMP},
	}

	table := &TableConfig{DataType: &testRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)

	shard := shardConfig{fieldMap: fieldMap, fields: fields}

	row := []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewTimestamp("2026-02-10 11:12:13"),
		sqltypes.NULL,
		sqltypes.NewTimestamp("2026-02-10 11:12:14"),
	}

	v := reflect.New(table.underlyingType)
	err = copyRowToStruct(shard, row, v)
	require.NoError(t, err)

	out := v.Interface().(*testRow)
	assert.Equal(t, int64(1), out.ID)
	assert.False(t, out.TS.IsZero())
	assert.Nil(t, out.Opt)
	require.NotNil(t, out.OptT)
	assert.False(t, out.OptT.IsZero())
}

func TestHandleRowEvent_TimeUsesConfiguredLocation(t *testing.T) {
	loc := time.FixedZone("UTC-5", -5*60*60)
	fields := []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT64},
		{Name: "ts", Type: querypb.Type_TIMESTAMP},
	}

	table := &TableConfig{Keyspace: "ks", Table: "t", DataType: &testRow{}, timeLocation: loc}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()
	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)
	table.shards = map[string]shardConfig{"0": {fieldMap: fieldMap, fields: fields}}
	table.resetBatch()

	ev := &binlogdatapb.RowEvent{TableName: "ks.t", Shard: "0", RowChanges: []*binlogdatapb.RowChange{{
		After: sqltypes.RowToProto3([]sqltypes.Value{
			sqltypes.NewInt64(1),
			sqltypes.NewTimestamp("2026-02-10 11:12:13"),
		}),
	}}}

	err = table.handleRowEvent(ev, &VStreamStats{})
	require.NoError(t, err)
	require.Len(t, table.currentBatch, 1)
	out, ok := table.currentBatch[0].Data.(*testRow)
	require.True(t, ok)
	assert.Equal(t, time.Date(2026, 2, 10, 11, 12, 13, 0, loc), out.TS)
	assert.Equal(t, loc, out.TS.Location())
}

func TestCopyRowToStruct_NullIntoNonPointerErrors(t *testing.T) {
	fields := []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}

	table := &TableConfig{DataType: &testRowSmall{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)

	shard := shardConfig{fieldMap: fieldMap, fields: fields}
	row := []sqltypes.Value{sqltypes.NULL}

	v := reflect.New(table.underlyingType)
	err = copyRowToStruct(shard, row, v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "NULL into non-pointer field")
}

func TestCopyRowToStruct_JSONFields(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT64},
		{Name: "payload", Type: querypb.Type_JSON},
		{Name: "opt_payload", Type: querypb.Type_JSON},
	}

	table := &TableConfig{DataType: &testJSONRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)

	shard := shardConfig{fieldMap: fieldMap, fields: fields}
	row := []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewVarBinary(`{"name":"alpha","count":2}`),
		sqltypes.NULL,
	}

	v := reflect.New(table.underlyingType)
	err = copyRowToStruct(shard, row, v)
	require.NoError(t, err)

	out := v.Interface().(*testJSONRow)
	assert.Equal(t, int64(1), out.ID)
	assert.Equal(t, "alpha", out.Payload.Name)
	assert.Equal(t, 2, out.Payload.Count)
	assert.Nil(t, out.OptPayload)
}

func TestCopyRowToStruct_JSONFieldInvalidErrors(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT64},
		{Name: "payload", Type: querypb.Type_JSON},
		{Name: "opt_payload", Type: querypb.Type_JSON},
	}

	table := &TableConfig{DataType: &testJSONRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)

	shard := shardConfig{fieldMap: fieldMap, fields: fields}
	row := []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewVarBinary(`{"name":"alpha"`),
		sqltypes.NULL,
	}

	v := reflect.New(table.underlyingType)
	err = copyRowToStruct(shard, row, v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "error unmarshalling JSON for field payload")
}

func TestCopyRowToStruct_JSONFieldNullIntoNonPointerErrors(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT64},
		{Name: "payload", Type: querypb.Type_JSON},
		{Name: "opt_payload", Type: querypb.Type_JSON},
	}

	table := &TableConfig{DataType: &testJSONRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)

	shard := shardConfig{fieldMap: fieldMap, fields: fields}
	row := []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NULL,
		sqltypes.NULL,
	}

	v := reflect.New(table.underlyingType)
	err = copyRowToStruct(shard, row, v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "error unmarshalling JSON for field payload")
	assert.ErrorContains(t, err, "NULL into non-pointer field")
}

func TestCopyRowToStruct_ByteAndRawJSONFields(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "payload", Type: querypb.Type_VARBINARY},
		{Name: "raw_payload", Type: querypb.Type_JSON},
		{Name: "opt_raw", Type: querypb.Type_JSON},
	}

	table := &TableConfig{DataType: &testBytesAndRawJSONRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)

	shard := shardConfig{fieldMap: fieldMap, fields: fields}
	rawPayload, err := sqltypes.NewJSON(`{"name":"alpha","count":2}`)
	require.NoError(t, err)
	row := []sqltypes.Value{
		sqltypes.NewVarBinary("payload-bytes"),
		rawPayload,
		sqltypes.NULL,
	}

	v := reflect.New(table.underlyingType)
	err = copyRowToStruct(shard, row, v)
	require.NoError(t, err)

	out := v.Interface().(*testBytesAndRawJSONRow)
	assert.Equal(t, []byte("payload-bytes"), out.Payload)
	assert.JSONEq(t, `{"name":"alpha","count":2}`, string(out.RawPayload))
	assert.Nil(t, out.OptRaw)
}

func TestCopyRowToStruct_NestedAndEmbeddedFields(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT64},
		{Name: "name", Type: querypb.Type_VARCHAR},
	}

	table := &TableConfig{DataType: &testNestedMappedRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)
	assert.Contains(t, fieldMap, "id")
	assert.Contains(t, fieldMap, "name")

	shard := shardConfig{fieldMap: fieldMap, fields: fields}
	row := []sqltypes.Value{
		sqltypes.NewInt64(7),
		sqltypes.NewVarChar("alpha"),
	}

	v := reflect.New(table.underlyingType)
	err = copyRowToStruct(shard, row, v)
	require.NoError(t, err)

	out := v.Interface().(*testNestedMappedRow)
	assert.Equal(t, int64(7), out.ID)
	assert.Equal(t, "alpha", out.Meta.Name)
}

func TestCopyRowToStruct_ScannerAndTextUnmarshalerFields(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "name", Type: querypb.Type_VARCHAR},
		{Name: "count", Type: querypb.Type_INT64},
		{Name: "seen_at", Type: querypb.Type_TIMESTAMP},
		{Name: "level", Type: querypb.Type_VARCHAR},
	}

	table := &TableConfig{DataType: &testWrapperRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)

	shard := shardConfig{fieldMap: fieldMap, fields: fields}
	row := []sqltypes.Value{
		sqltypes.NewVarChar("alpha"),
		sqltypes.NewInt64(42),
		sqltypes.NewTimestamp("2026-02-10 11:12:13"),
		sqltypes.NewVarChar("gold"),
	}

	v := reflect.New(table.underlyingType)
	err = copyRowToStruct(shard, row, v)
	require.NoError(t, err)

	out := v.Interface().(*testWrapperRow)
	assert.True(t, out.Name.Valid)
	assert.Equal(t, "alpha", out.Name.String)
	assert.True(t, out.Count.Valid)
	assert.Equal(t, int64(42), out.Count.Int64)
	assert.True(t, out.SeenAt.Valid)
	assert.Equal(t, time.Date(2026, 2, 10, 11, 12, 13, 0, time.UTC), out.SeenAt.Time)
	assert.Equal(t, testTextWrapper("wrapped:gold"), out.Level)
}

func TestCopyRowToStruct_UnsupportedStructFieldErrors(t *testing.T) {
	fields := []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "wrapped", Type: querypb.Type_VARCHAR}}

	table := &TableConfig{DataType: &testStructFieldRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)

	shard := shardConfig{fieldMap: fieldMap, fields: fields}
	row := []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewVarChar("value")}

	v := reflect.New(table.underlyingType)
	err = copyRowToStruct(shard, row, v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unsupported struct field type")
	assert.ErrorContains(t, err, "wrapped")
}

func TestCopyRowToStruct_IntOverflowErrors(t *testing.T) {
	fields := []*querypb.Field{{Name: "value", Type: querypb.Type_INT64}}

	table := &TableConfig{DataType: &testSmallIntRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)

	shard := shardConfig{fieldMap: fieldMap, fields: fields}
	row := []sqltypes.Value{sqltypes.NewInt64(128)}

	v := reflect.New(table.underlyingType)
	err = copyRowToStruct(shard, row, v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "overflows destination type")
	assert.ErrorContains(t, err, "value")
}

func TestCopyRowToStruct_UintOverflowErrors(t *testing.T) {
	fields := []*querypb.Field{{Name: "value", Type: querypb.Type_UINT64}}

	table := &TableConfig{DataType: &testSmallUintRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)

	shard := shardConfig{fieldMap: fieldMap, fields: fields}
	row := []sqltypes.Value{sqltypes.NewUint64(256)}

	v := reflect.New(table.underlyingType)
	err = copyRowToStruct(shard, row, v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "overflows destination type")
	assert.ErrorContains(t, err, "value")
}

func TestCopyRowToStruct_FloatOverflowErrors(t *testing.T) {
	fields := []*querypb.Field{{Name: "value", Type: querypb.Type_FLOAT64}}

	table := &TableConfig{DataType: &testSmallFloatRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)

	shard := shardConfig{fieldMap: fieldMap, fields: fields}
	row := []sqltypes.Value{sqltypes.NewFloat64(1e40)}

	v := reflect.New(table.underlyingType)
	err = copyRowToStruct(shard, row, v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "overflows destination type")
	assert.ErrorContains(t, err, "value")
}

func TestReflectMapFields_ErrorOnUnknownFields(t *testing.T) {
	fields := []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "extra", Type: querypb.Type_INT64}}

	table := &TableConfig{DataType: &testRowSmall{}, ErrorOnUnknownFields: true}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	_, err := table.reflectMapFields(fields)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "field extra")
}

func TestReflectMapFields_FallbackTagsStripOptions(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "db_name", Type: querypb.Type_VARCHAR},
		{Name: "json_name", Type: querypb.Type_VARCHAR},
		{Name: "option_name", Type: querypb.Type_VARCHAR},
	}

	table := &TableConfig{DataType: &testFallbackTagRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)

	_, ok := fieldMap["db_name"]
	assert.True(t, ok)
	_, ok = fieldMap["json_name"]
	assert.True(t, ok)
	m, ok := fieldMap["option_name"]
	require.True(t, ok)
	assert.True(t, m.jsonDecode)
}

func TestInitTables_RequiresFlushFn(t *testing.T) {
	v := &VStreamClient{
		shardsByKeyspace: map[string][]string{"ks": {"0"}},
		tables:           make(map[string]*TableConfig),
	}

	err := v.initTables([]TableConfig{{
		Keyspace: "ks",
		Table:    "t",
		DataType: &testRowSmall{},
	}})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "has no flush function")
}

func TestInitTables_SetsDefaults(t *testing.T) {
	v := &VStreamClient{
		shardsByKeyspace: map[string][]string{"ks": {"0"}},
		tables:           make(map[string]*TableConfig),
	}

	err := v.initTables([]TableConfig{{
		Keyspace: "ks",
		Table:    "t",
		DataType: &testRowSmall{},
		FlushFn:  func(context.Context, []Row, FlushMeta) error { return nil },
	}})
	require.NoError(t, err)

	table := v.tables[qualifiedTableName("ks", "t")]
	require.NotNil(t, table)
	assert.Equal(t, "select * from `t`", table.Query)
	assert.Equal(t, DefaultMaxRowsPerFlush, table.MaxRowsPerFlush)
	assert.Len(t, table.currentBatch, 0)
	assert.Equal(t, DefaultMaxRowsPerFlush, cap(table.currentBatch))
}

func TestInitTables_RejectsDuplicateTables(t *testing.T) {
	v := &VStreamClient{
		shardsByKeyspace: map[string][]string{"ks1": {"0"}, "ks2": {"0"}},
		tables:           make(map[string]*TableConfig),
	}

	err := v.initTables([]TableConfig{
		{Keyspace: "ks1", Table: "t", DataType: &testRowSmall{}, FlushFn: func(context.Context, []Row, FlushMeta) error { return nil }},
		{Keyspace: "ks2", Table: "t", DataType: &testRowSmall{}, FlushFn: func(context.Context, []Row, FlushMeta) error { return nil }},
	})
	require.NoError(t, err)
	assert.Contains(t, v.tables, qualifiedTableName("ks1", "t"))
	assert.Contains(t, v.tables, qualifiedTableName("ks2", "t"))
}

func TestInitTables_RejectsConflictingQueriesForSameTableAcrossKeyspaces(t *testing.T) {
	v := &VStreamClient{
		shardsByKeyspace: map[string][]string{"ks1": {"0"}, "ks2": {"0"}},
		tables:           make(map[string]*TableConfig),
	}

	err := v.initTables([]TableConfig{
		{Keyspace: "ks1", Table: "t", Query: "select * from t where id < 10", DataType: &testRowSmall{}, FlushFn: func(context.Context, []Row, FlushMeta) error { return nil }},
		{Keyspace: "ks2", Table: "t", Query: "select * from t where id >= 10", DataType: &testRowSmall{}, FlushFn: func(context.Context, []Row, FlushMeta) error { return nil }},
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "same table name across keyspaces must use identical queries")
}

func TestInitTables_RejectsUnknownKeyspace(t *testing.T) {
	v := &VStreamClient{
		shardsByKeyspace: map[string][]string{"ks": {"0"}},
		tables:           make(map[string]*TableConfig),
	}

	err := v.initTables([]TableConfig{{
		Keyspace: "missing",
		Table:    "t",
		DataType: &testRowSmall{},
		FlushFn:  func(context.Context, []Row, FlushMeta) error { return nil },
	}})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "keyspace missing not found")
}

func TestInitTables_RejectsNonStructDataType(t *testing.T) {
	v := &VStreamClient{
		shardsByKeyspace: map[string][]string{"ks": {"0"}},
		tables:           make(map[string]*TableConfig),
	}

	err := v.initTables([]TableConfig{{
		Keyspace: "ks",
		Table:    "t",
		DataType: new(int64),
		FlushFn:  func(context.Context, []Row, FlushMeta) error { return nil },
	}})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "must be a struct")
}

func TestInitTables_RejectsTypedNilDataType(t *testing.T) {
	v := &VStreamClient{
		shardsByKeyspace: map[string][]string{"ks": {"0"}},
		tables:           make(map[string]*TableConfig),
	}

	var row *testRowSmall
	err := v.initTables([]TableConfig{{
		Keyspace: "ks",
		Table:    "t",
		DataType: row,
		FlushFn:  func(context.Context, []Row, FlushMeta) error { return nil },
	}})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "has no data type")
}

func TestInitTables_RejectsNegativeMaxRowsPerFlush(t *testing.T) {
	v := &VStreamClient{
		shardsByKeyspace: map[string][]string{"ks": {"0"}},
		tables:           make(map[string]*TableConfig),
	}

	err := v.initTables([]TableConfig{{
		Keyspace:        "ks",
		Table:           "t",
		MaxRowsPerFlush: -1,
		DataType:        &testRowSmall{},
		FlushFn:         func(context.Context, []Row, FlushMeta) error { return nil },
	}})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "max rows per flush must be positive")
}

func TestValidateTableConfig(t *testing.T) {
	t.Run("matching config passes", func(t *testing.T) {
		err := validateTableConfig(
			map[string]*TableConfig{"t": {Keyspace: "ks", Table: "t", Query: "select * from t"}},
			map[string]*TableConfig{"t": {Keyspace: "ks", Table: "t", Query: "select * from t"}},
		)
		require.NoError(t, err)
	})

	t.Run("different config fails", func(t *testing.T) {
		err := validateTableConfig(
			map[string]*TableConfig{"t": {Keyspace: "ks", Table: "t", Query: "select * from t"}},
			map[string]*TableConfig{"t": {Keyspace: "ks", Table: "t", Query: "select id from t"}},
		)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "provided tables do not match stored tables")
		assert.ErrorContains(t, err, "table ks.t query changed")
		assert.ErrorContains(t, err, "provided \"select * from t\"")
		assert.ErrorContains(t, err, "stored \"select id from t\"")
	})

	t.Run("missing stored table is reported", func(t *testing.T) {
		err := validateTableConfig(
			map[string]*TableConfig{
				"t1": {Keyspace: "ks", Table: "t1", Query: "select * from t1"},
				"t2": {Keyspace: "ks", Table: "t2", Query: "select * from t2"},
			},
			map[string]*TableConfig{
				"t1": {Keyspace: "ks", Table: "t1", Query: "select * from t1"},
			},
		)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "table ks.t2 is new in provided config")
	})

	t.Run("missing provided table is reported", func(t *testing.T) {
		err := validateTableConfig(
			map[string]*TableConfig{
				"t1": {Keyspace: "ks", Table: "t1", Query: "select * from t1"},
			},
			map[string]*TableConfig{
				"t1": {Keyspace: "ks", Table: "t1", Query: "select * from t1"},
				"t2": {Keyspace: "ks", Table: "t2", Query: "select * from t2"},
			},
		)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "table ks.t2 is missing from provided config")
	})
}

func TestHandleRowEvent_DeleteUsesBeforeRowData(t *testing.T) {
	fields := []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}

	table := &TableConfig{Keyspace: "ks", Table: "t", DataType: &testRowSmall{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()
	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)

	table.shards = map[string]shardConfig{"0": {fieldMap: fieldMap, fields: fields}}
	table.resetBatch()

	ev := &binlogdatapb.RowEvent{TableName: "t", Shard: "0", RowChanges: []*binlogdatapb.RowChange{{
		Before: &querypb.Row{Lengths: []int64{1}, Values: []byte("1")},
		After:  nil,
	}}}

	stats := &VStreamStats{}
	err = table.handleRowEvent(ev, stats)
	require.NoError(t, err)
	require.Len(t, table.currentBatch, 1)
	row, ok := table.currentBatch[0].Data.(*testRowSmall)
	require.True(t, ok)
	assert.Equal(t, int64(1), row.ID)
}

func TestHandleRowEvent_InsertScansNullableFields(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT64},
		{Name: "opt", Type: querypb.Type_INT64},
		{Name: "payload", Type: querypb.Type_JSON},
	}

	table := &TableConfig{Keyspace: "ks", Table: "t", DataType: &testNullableJSONRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()
	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)
	table.shards = map[string]shardConfig{"0": {fieldMap: fieldMap, fields: fields}}
	table.resetBatch()

	ev := &binlogdatapb.RowEvent{
		TableName: "ks.t",
		Shard:     "0",
		RowChanges: []*binlogdatapb.RowChange{{
			After: sqltypes.RowToProto3([]sqltypes.Value{
				sqltypes.NewInt64(7),
				sqltypes.NULL,
				sqltypes.NewVarBinary(`{"name":"beta","count":4}`),
			}),
		}},
	}

	stats := &VStreamStats{}
	err = table.handleRowEvent(ev, stats)
	require.NoError(t, err)
	require.Len(t, table.currentBatch, 1)
	row, ok := table.currentBatch[0].Data.(*testNullableJSONRow)
	require.True(t, ok)
	assert.Equal(t, int64(7), row.ID)
	assert.Nil(t, row.Opt)
	require.NotNil(t, row.Payload)
	assert.Equal(t, "beta", row.Payload.Name)
	assert.Equal(t, 4, row.Payload.Count)
}

func TestHandleRowEvent_UsesTypedScanner(t *testing.T) {
	fields := []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}

	table := &TableConfig{Keyspace: "ks", Table: "t", DataType: &testScannerRow{}}
	table.implementsScanner = true
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	table.shards = map[string]shardConfig{"0": {fieldMap: nil, fields: fields}}
	table.resetBatch()

	ev := &binlogdatapb.RowEvent{TableName: "t", Shard: "0", RowChanges: []*binlogdatapb.RowChange{{
		Before: nil,
		After:  &querypb.Row{Lengths: []int64{1}, Values: []byte("7")},
	}}}

	stats := &VStreamStats{}
	err := table.handleRowEvent(ev, stats)
	require.NoError(t, err)
	require.Len(t, table.currentBatch, 1)
	row := table.currentBatch[0]
	scanned, ok := row.Data.(*testScannerRow)
	require.True(t, ok)
	assert.Equal(t, int64(7), scanned.ID)
}

func TestInitTables_ValueDataTypeStillUsesPointerScanner(t *testing.T) {
	v := &VStreamClient{
		shardsByKeyspace: map[string][]string{"ks": {"0"}},
		tables:           make(map[string]*TableConfig),
	}

	err := v.initTables([]TableConfig{{
		Keyspace: "ks",
		Table:    "t",
		DataType: testScannerRow{},
		FlushFn:  func(context.Context, []Row, FlushMeta) error { return nil },
	}})
	require.NoError(t, err)

	table := v.tables[qualifiedTableName("ks", "t")]
	require.NotNil(t, table)
	assert.True(t, table.implementsScanner)

	err = table.handleFieldEvent(&binlogdatapb.FieldEvent{
		Shard:  "0",
		Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
	})
	require.NoError(t, err)

	err = table.handleRowEvent(&binlogdatapb.RowEvent{
		TableName: "t",
		Shard:     "0",
		RowChanges: []*binlogdatapb.RowChange{{
			After: &querypb.Row{Lengths: []int64{1}, Values: []byte("7")},
		}},
	}, &VStreamStats{})
	require.NoError(t, err)

	require.Len(t, table.currentBatch, 1)
	scanned, ok := table.currentBatch[0].Data.(*testScannerRow)
	require.True(t, ok)
	assert.Equal(t, int64(7), scanned.ID)
}

func TestHandleRowEvent_DecodeFailureDoesNotBufferRow(t *testing.T) {
	fields := []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}

	table := &TableConfig{Keyspace: "ks", Table: "t", DataType: &testRowSmall{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()
	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)
	table.shards = map[string]shardConfig{"0": {fieldMap: fieldMap, fields: fields}}
	table.resetBatch()

	ev := &binlogdatapb.RowEvent{TableName: "t", Shard: "0", RowChanges: []*binlogdatapb.RowChange{{
		After: &querypb.Row{Lengths: []int64{3}, Values: []byte("bad")},
	}}}

	stats := &VStreamStats{}
	err = table.handleRowEvent(ev, stats)
	assert.Error(t, err)
	assert.Len(t, table.currentBatch, 0)
}

func TestHandleRowEvent_ScannerFailureDoesNotBufferRow(t *testing.T) {
	fields := []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}}

	table := &TableConfig{Keyspace: "ks", Table: "t", DataType: &testFailingScannerRow{}}
	table.implementsScanner = true
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()
	table.shards = map[string]shardConfig{"0": {fields: fields}}
	table.resetBatch()

	ev := &binlogdatapb.RowEvent{TableName: "t", Shard: "0", RowChanges: []*binlogdatapb.RowChange{{
		After: &querypb.Row{Lengths: []int64{1}, Values: []byte("7")},
	}}}

	stats := &VStreamStats{}
	err := table.handleRowEvent(ev, stats)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "client scan failed")
	assert.Len(t, table.currentBatch, 0)
}

func TestHandleRowEvent_PartialRowImageFailsForDefaultDecoder(t *testing.T) {
	fields := []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}, {Name: "email", Type: querypb.Type_VARCHAR}}

	table := &TableConfig{Keyspace: "ks", Table: "t", DataType: &testRowSmall{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()
	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)
	table.shards = map[string]shardConfig{"0": {fieldMap: fieldMap, fields: fields}}
	table.resetBatch()

	ev := &binlogdatapb.RowEvent{TableName: "ks.t", Shard: "0", RowChanges: []*binlogdatapb.RowChange{{
		After:       sqltypes.RowToProto3([]sqltypes.Value{sqltypes.NewInt64(7)}),
		DataColumns: &binlogdatapb.RowChange_Bitmap{Count: 2, Cols: []byte{0x01}},
	}}}

	stats := &VStreamStats{}
	err = table.handleRowEvent(ev, stats)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "partial row images are unsupported")
	assert.Len(t, table.currentBatch, 0)
}

func TestHandleRowEvent_PartialJSONFailsForDefaultDecoder(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT64},
		{Name: "payload", Type: querypb.Type_JSON},
		{Name: "opt_payload", Type: querypb.Type_JSON},
	}

	table := &TableConfig{Keyspace: "ks", Table: "t", DataType: &testJSONRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()
	fieldMap, err := table.reflectMapFields(fields)
	require.NoError(t, err)
	table.shards = map[string]shardConfig{"0": {fieldMap: fieldMap, fields: fields}}
	table.resetBatch()

	ev := &binlogdatapb.RowEvent{TableName: "ks.t", Shard: "0", RowChanges: []*binlogdatapb.RowChange{{
		After: sqltypes.RowToProto3([]sqltypes.Value{
			sqltypes.NewInt64(1),
			sqltypes.TestValue(sqltypes.Expression, "JSON_SET(payload, '$.name', 'beta')"),
			sqltypes.NULL,
		}),
		DataColumns:       &binlogdatapb.RowChange_Bitmap{Count: 3, Cols: []byte{0x03}},
		JsonPartialValues: &binlogdatapb.RowChange_Bitmap{Count: 3, Cols: []byte{0x02}},
	}}}

	stats := &VStreamStats{}
	err = table.handleRowEvent(ev, stats)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "partial JSON updates are unsupported")
	assert.Len(t, table.currentBatch, 0)
}

func TestWithFlags_RejectsZeroHeartbeatInterval(t *testing.T) {
	v := &VStreamClient{}
	err := WithFlags(&vtgatepb.VStreamFlags{HeartbeatInterval: 0})(v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "HeartbeatInterval")
}
