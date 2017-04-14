package splitquery

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
)

var splitParamsTestCases = []struct {
	SQL                 string
	BindVariables       map[string]interface{}
	SplitColumnNames    []sqlparser.ColIdent
	NumRowsPerQueryPart int64
	SplitCount          int64
	Schema              map[string]*schema.Table

	ExpectedErrorRegex  *regexp.Regexp
	ExpectedSplitParams SplitParams
}{
	{ // Test NewSplitParamsGivenSplitCount; correct input.
		SQL:              "select id from test_table",
		BindVariables:    map[string]interface{}{"foo": "123"},
		SplitColumnNames: []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
		SplitCount:       100,
		Schema:           getTestSchema(),

		ExpectedSplitParams: SplitParams{
			splitCount:          100,
			numRowsPerQueryPart: 10, // TableRows of 'test_table' should be 1000
			splitColumns:        []*schema.TableColumn{getTestSchemaColumn("test_table", "id")},
			splitTableSchema:    testSchema["test_table"],
		},
	},
	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; correct input.
		SQL:                 "select user_id from test_table",
		BindVariables:       map[string]interface{}{"foo": "123"},
		SplitColumnNames:    []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
		NumRowsPerQueryPart: 100,
		Schema:              getTestSchema(),

		ExpectedSplitParams: SplitParams{
			splitCount:          10,
			numRowsPerQueryPart: 100, // TableRows of 'test_table' should be 1000
			splitColumns:        []*schema.TableColumn{getTestSchemaColumn("test_table", "id")},
			splitTableSchema:    testSchema["test_table"],
		},
	},
	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; correct input; default split columns
		SQL:                 "select user_id from test_table",
		BindVariables:       map[string]interface{}{"foo": "123"},
		NumRowsPerQueryPart: 100,
		Schema:              getTestSchema(),

		ExpectedSplitParams: SplitParams{
			splitCount:          10,
			numRowsPerQueryPart: 100, // TableRows of 'test_table' should be 1000
			splitColumns: []*schema.TableColumn{
				getTestSchemaColumn("test_table", "id"),
				getTestSchemaColumn("test_table", "user_id"),
			},
			splitTableSchema: testSchema["test_table"],
		},
	},

	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; invalid query.
		SQL:                 "not a valid query",
		BindVariables:       map[string]interface{}{"foo": "123"},
		SplitColumnNames:    []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
		NumRowsPerQueryPart: 100,
		Schema:              getTestSchema(),

		ExpectedErrorRegex: regexp.MustCompile("failed parsing query: 'not a valid query'"),
	},
	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; not a select statement.
		SQL:                 "delete from test_table",
		BindVariables:       map[string]interface{}{"foo": "123"},
		SplitColumnNames:    []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
		NumRowsPerQueryPart: 100,
		Schema:              getTestSchema(),

		ExpectedErrorRegex: regexp.MustCompile("not a select statement"),
	},
	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; unsupported select statement.
		SQL:                 "select t1.user_id from test_table as t1 join test_table as t2",
		BindVariables:       map[string]interface{}{"foo": "123"},
		SplitColumnNames:    []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
		NumRowsPerQueryPart: 100,
		Schema:              getTestSchema(),

		ExpectedErrorRegex: regexp.MustCompile("unsupported FROM clause"),
	},
	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; unsupported select statement.
		SQL:                 "select distinct user_id from test_table",
		BindVariables:       map[string]interface{}{"foo": "123"},
		SplitColumnNames:    []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
		NumRowsPerQueryPart: 100,
		Schema:              getTestSchema(),

		ExpectedErrorRegex: regexp.MustCompile("unsupported query"),
	},
	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; unsupported select statement.
		SQL:                 "select user_id from test_table group by id",
		BindVariables:       map[string]interface{}{"foo": "123"},
		SplitColumnNames:    []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
		NumRowsPerQueryPart: 100,
		Schema:              getTestSchema(),

		ExpectedErrorRegex: regexp.MustCompile("unsupported query"),
	},
	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; unsupported select statement.
		SQL:                 "select user_id from test_table having user_id > 5",
		BindVariables:       map[string]interface{}{"foo": "123"},
		SplitColumnNames:    []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
		NumRowsPerQueryPart: 100,
		Schema:              getTestSchema(),

		ExpectedErrorRegex: regexp.MustCompile("unsupported query"),
	},
	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; unsupported select statement.
		SQL:                 "select user_id from test_table order by id asc",
		BindVariables:       map[string]interface{}{"foo": "123"},
		SplitColumnNames:    []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
		NumRowsPerQueryPart: 100,
		Schema:              getTestSchema(),

		ExpectedErrorRegex: regexp.MustCompile("unsupported query"),
	},
	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; unsupported select statement.
		SQL:                 "select user_id from test_table lock in share mode",
		BindVariables:       map[string]interface{}{"foo": "123"},
		SplitColumnNames:    []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
		NumRowsPerQueryPart: 100,
		Schema:              getTestSchema(),

		ExpectedErrorRegex: regexp.MustCompile("unsupported query"),
	},
	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; unsupported select statement.
		SQL:                 "select user_id from (select * from test_table) as t1",
		BindVariables:       map[string]interface{}{"foo": "123"},
		SplitColumnNames:    []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
		NumRowsPerQueryPart: 100,
		Schema:              getTestSchema(),

		ExpectedErrorRegex: regexp.MustCompile("unsupported FROM clause"),
	},
	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; unknown table.
		SQL:                 "select user_id from missing_table",
		BindVariables:       map[string]interface{}{"foo": "123"},
		SplitColumnNames:    []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
		NumRowsPerQueryPart: 100,
		Schema:              getTestSchema(),

		ExpectedErrorRegex: regexp.MustCompile("can't find table in schema"),
	},
	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; unknown split column.
		SQL:                 "select * from test_table",
		BindVariables:       map[string]interface{}{"foo": "123"},
		SplitColumnNames:    []sqlparser.ColIdent{sqlparser.NewColIdent("missing_column")},
		NumRowsPerQueryPart: 100,
		Schema:              getTestSchema(),

		ExpectedErrorRegex: regexp.MustCompile("can't find split column"),
	},
	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; split columns not a prefix of an index.
		SQL:                 "select * from test_table",
		BindVariables:       map[string]interface{}{"foo": "123"},
		SplitColumnNames:    []sqlparser.ColIdent{sqlparser.NewColIdent("float32_col")},
		NumRowsPerQueryPart: 100,
		Schema:              getTestSchema(),

		ExpectedErrorRegex: regexp.MustCompile(
			"split-columns must be a prefix of the columns composing an index"),
	},
	{ // Test NewSplitParamsGivenNumRowsPerQueryPart; no split columns and no primary keys.
		SQL:                 "select id from test_table",
		BindVariables:       map[string]interface{}{"foo": "123"},
		SplitColumnNames:    []sqlparser.ColIdent{},
		NumRowsPerQueryPart: 100,
		Schema: func() map[string]*schema.Table {
			// Returns the testSchema without the primary key columns.
			result := getTestSchema()
			result["test_table"].PKColumns = []int{}
			return result
		}(),

		ExpectedErrorRegex: regexp.MustCompile(
			"no split columns where given and the queried table has no primary key columns"),
	},
}

func TestSplitParams(t *testing.T) {
	for _, testCase := range splitParamsTestCases {
		var splitParams *SplitParams
		var err error
		if testCase.NumRowsPerQueryPart != 0 {
			splitParams, err = NewSplitParamsGivenNumRowsPerQueryPart(
				querytypes.BoundQuery{
					Sql:           testCase.SQL,
					BindVariables: testCase.BindVariables,
				},
				testCase.SplitColumnNames,
				testCase.NumRowsPerQueryPart,
				testCase.Schema)
		} else {
			splitParams, err = NewSplitParamsGivenSplitCount(
				querytypes.BoundQuery{
					Sql:           testCase.SQL,
					BindVariables: testCase.BindVariables,
				},
				testCase.SplitColumnNames,
				testCase.SplitCount,
				testCase.Schema)
		}
		if testCase.ExpectedErrorRegex != nil {
			if !testCase.ExpectedErrorRegex.MatchString(err.Error()) {
				t.Errorf("Testcase: %+v, want: %+v, got: %+v", testCase, testCase.ExpectedErrorRegex, err)
			}
			continue
		}
		// Here, we don't expect an error.
		if err != nil {
			t.Errorf("TestCase: %+v, want: %+v, got: %+v", testCase, nil, err)
			continue
		}
		if splitParams == nil {
			t.Errorf("TestCase: %+v, got nil splitParams", testCase)
			continue
		}
		expectedSplitParams := testCase.ExpectedSplitParams
		// We don't require testCaset.ExpectedSplitParams to specify common expected fields like 'sql',
		// so we compute them here and store them in 'expectedSplitParams'.
		expectedSplitParams.sql = testCase.SQL
		expectedSplitParams.bindVariables = testCase.BindVariables
		statement, _ := sqlparser.Parse(testCase.SQL)
		expectedSplitParams.selectAST = statement.(*sqlparser.Select)
		if !reflect.DeepEqual(&expectedSplitParams, splitParams) {
			t.Errorf("TestCase: %+v, want: %+v, got: %+v", testCase, expectedSplitParams, *splitParams)
		}
	}
}
