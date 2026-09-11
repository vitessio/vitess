/*
Copyright 2026 The Vitess Authors.

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
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// benchWideRow mirrors a typical CDC destination struct: mostly integers, a
// couple of timestamps, and no fields implementing sql.Scanner or
// encoding.TextUnmarshaler. That last part is the common case, and it is the
// case that previously paid two reflect.Type.Implements calls per field per row.
type benchWideRow struct {
	ProjectPullID        int64     `vstream:"project_pull_id"`
	WorkspaceID          int64     `vstream:"workspace_id"`
	ProjectID            int64     `vstream:"project_id"`
	PullID               int64     `vstream:"pull_id"`
	RankingID            int64     `vstream:"ranking_id"`
	Requested            int64     `vstream:"requested"`
	KeywordID            int64     `vstream:"keyword_id"`
	RankingLastUpdatedAt int64     `vstream:"ranking_last_updated_at"`
	LastInsertedAt       int64     `vstream:"last_inserted_at"`
	InsertedCount        int32     `vstream:"inserted_count"`
	CreatedAt            time.Time `vstream:"created_at"`
}

func benchWideFields() []*querypb.Field {
	return []*querypb.Field{
		{Name: "project_pull_id", Type: querypb.Type_INT64},
		{Name: "workspace_id", Type: querypb.Type_INT64},
		{Name: "project_id", Type: querypb.Type_INT64},
		{Name: "pull_id", Type: querypb.Type_INT64},
		{Name: "ranking_id", Type: querypb.Type_INT64},
		{Name: "requested", Type: querypb.Type_INT64},
		{Name: "keyword_id", Type: querypb.Type_INT64},
		{Name: "ranking_last_updated_at", Type: querypb.Type_INT64},
		{Name: "last_inserted_at", Type: querypb.Type_INT64},
		{Name: "inserted_count", Type: querypb.Type_INT32},
		{Name: "created_at", Type: querypb.Type_TIMESTAMP},
	}
}

func benchWideValues() []sqltypes.Value {
	return []sqltypes.Value{
		sqltypes.NewInt64(101),
		sqltypes.NewInt64(102),
		sqltypes.NewInt64(103),
		sqltypes.NewInt64(104),
		sqltypes.NewInt64(105),
		sqltypes.NewInt64(1753972645),
		sqltypes.NewInt64(106),
		sqltypes.NewInt64(107),
		sqltypes.NewInt64(108),
		sqltypes.NewInt32(109),
		sqltypes.NewTimestamp("2026-07-02 03:04:05"),
	}
}

func BenchmarkCopyRowToStruct(b *testing.B) {
	fields := benchWideFields()
	table := &TableConfig{DataType: &benchWideRow{}}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

	fieldMappings, err := table.reflectMapFields(fields)
	require.NoError(b, err)

	shard := shardConfig{fieldMappings: fieldMappings, fields: fields}
	row := benchWideValues()
	dest := reflect.New(table.underlyingType)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if copyErr := copyRowToStruct(shard, row, dest); copyErr != nil {
			b.Fatal(copyErr)
		}
	}
}
