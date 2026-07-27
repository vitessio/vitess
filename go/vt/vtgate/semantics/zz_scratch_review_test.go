package semantics

import (
	"testing"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestZZReviewRepros(t *testing.T) {
	authSI := &FakeSI{Tables: map[string]*vindexes.BaseTable{
		"authoritative": {
			Name: sqlparser.NewIdentifierCS("authoritative"),
			Columns: []vindexes.Column{
				{Name: sqlparser.NewIdentifierCI("col1"), Type: querypb.Type_INT64},
				{Name: sqlparser.NewIdentifierCI("col2"), Type: querypb.Type_INT64},
				{Name: sqlparser.NewIdentifierCI("col3"), Type: querypb.Type_INT64},
			},
			ColumnListAuthoritative: true,
			Keyspace:                unsharded,
		},
	}}
	cases := []struct {
		name, query string
		si          *FakeSI
	}{
		{"F1 recursive chain unused", "with recursive x(a) as (select 1, 2), y as (select 1 from x) select 1", fakeSchemaInfo()},
		{"F1 plain chain unused", "with x(a) as (select 1, 2), y as (select 1 from x) select 1", fakeSchemaInfo()},
		{"F1 control chain used", "with x(a) as (select 1, 2), y as (select 1 from x) select * from y", fakeSchemaInfo()},
		{"F2 qualified star 3 names", "select a from (select r.* from authoritative l join authoritative r using (col1)) x(a, b, c)", authSI},
		{"F2 qualified star 2 names", "select a from (select r.* from authoritative l join authoritative r using (col1)) x(a, b)", authSI},
		{"F2 unqualified star 5 names", "select a from (select * from authoritative l join authoritative r using (col1)) x(a, b, c, d, e)", authSI},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parse, err := sqlparser.NewTestParser().Parse(tc.query)
			require.NoError(t, err)
			_, err = AnalyzeStrict(parse, "user", tc.si)
			t.Logf("err=%v", err)
			t.Logf("rewritten=%s", sqlparser.String(parse))
		})
	}
}
