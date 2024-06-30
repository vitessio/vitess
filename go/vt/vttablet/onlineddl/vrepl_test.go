/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package onlineddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/schemadiff"
)

func TestReadTableColumns(t *testing.T) {
	tcases := []struct {
		name      string
		create    string
		cols      []string
		generated []string
		pk        []string
	}{
		{
			name: "simple",
			create: `create table t (
				id int,
				primary key (id)
			)`,
			cols:      []string{"id"},
			generated: []string{},
			pk:        []string{"id"},
		},
		{
			name: "complex",
			create: `create table t (
				id int,
				col1 int,
				col2 int,
				col3 int generated always as (col1 + 1) stored,
				col4 int generated always as (col1 + 1) virtual,
				col5 int,
				primary key (id, col1),
				unique key (id, col5),
				unique key (col2, col5),
				key (col5)
			)`,
			cols:      []string{"id", "col1", "col2", "col3", "col4", "col5"},
			generated: []string{"col3", "col4"},
			pk:        []string{"id", "col1"},
		},
	}
	env := schemadiff.NewTestEnv()
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			createTableEntity, err := schemadiff.NewCreateTableEntityFromSQL(env, tcase.create)
			require.NoError(t, err)
			cols, virtual, pk, err := getTableColumns(createTableEntity)
			assert.NoError(t, err)
			assert.Equal(t, tcase.cols, cols.Names())
			assert.Equal(t, tcase.generated, virtual.Names())
			assert.Equal(t, tcase.pk, pk.Names())
		})
	}
}
