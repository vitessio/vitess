/*
Copyright 2023 The Vitess Authors.

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

package sqltypes

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type T1 struct {
	Name    string
	Age     int
	Tablet  *topodatapb.TabletAlias
	AddedAt time.Time
	Period  time.Duration
}

type T2 T1

func (t2 *T2) MarshalResult() (*Result, error) {
	tmp := struct {
		*T1
		Tablet_        string `sqltypes:"$$tablet"`
		AddedTimestamp time.Time
		PeriodSeconds  int
	}{
		T1:             (*T1)(t2),
		Tablet_:        topoproto.TabletAliasString(t2.Tablet),
		AddedTimestamp: t2.AddedAt,
		PeriodSeconds:  int(t2.Period.Seconds()),
	}

	res, err := MarshalResult(&tmp)
	if err != nil {
		return nil, err
	}

	return ReplaceFields(res, map[string]string{
		// Replace `period`/'added_at` field and column values.
		"period":   "period_seconds",
		"added_at": "added_timestamp",
		// Replace `tablet` column values only.
		"$$tablet": "tablet",
	}), nil
}

func TestMarshalResult(t *testing.T) {
	t.Parallel()

	now := time.Now()
	t1 := &T1{
		Name: "test",
		Age:  10,
		Tablet: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  100,
		},
		AddedAt: now,
		Period:  time.Minute,
	}

	r, err := MarshalResult((*T2)(t1))
	require.NoError(t, err)

	row := r.Named().Rows[0]

	assert.Equal(t, "test", row.AsString("name", ""))
	assert.Equal(t, int64(10), row.AsInt64("age", 0))
	assert.Equal(t, "zone1-0000000100", row.AsString("tablet", ""))
	assert.Equal(t, now.Format(TimestampFormat), row.AsString("added_timestamp", ""))
	assert.Equal(t, int64(60), row.AsInt64("period_seconds", 0))

	// fields we renamed/remapped are not present
	assert.Empty(t, row.AsString("$$tablet", ""))
	assert.Empty(t, row.AsString("added_at", ""))
	assert.Empty(t, row.AsString("period", ""))
}

func TestSnakeCase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in, out string
	}{
		{"Foo", "foo"},
		{"FooBar", "foo_bar"},
	}

	for _, test := range tests {
		t.Run(test.in, func(t *testing.T) {
			assert.Equal(t, test.out, snakeCase(test.in))
		})
	}
}
