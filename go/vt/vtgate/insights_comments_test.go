package vtgate

import (
	"testing"

	pbvtgate "github.com/planetscale/psevents/go/vtgate/v1"

	"github.com/stretchr/testify/assert"
)

func TestCommentSplitting(t *testing.T) {
	testCases := []struct {
		input    string
		output   string
		comments []string
	}{
		{
			"hello world /* comment */",
			"hello world",
			[]string{"comment"},
		},
		{
			"hello world /* unterminated comment",
			"hello world /* unterminated comment",
			nil,
		},
		{
			"before/* comment */after",
			"before/* comment */after",
			nil,
		},
		{
			"/* now */ hello /* three */ world /* comments */",
			"hello /* three */ world",
			[]string{"now", "comments"},
		},
		{
			"/*/*/*/*///***/*/***///**/",
			"*/*///***/*/***//",
			[]string{"/", ""},
		},
		{
			" no\tcomments\t",
			"no\tcomments",
			nil,
		},
		{
			"",
			"",
			nil,
		},
		{
			// We don't split on `--` because that style of comments gets split off before the
			// string is copied into LogStats.SQL.  Only `/* ... */` comments show up in LogStats.SQL.
			"we don't -- split these",
			"we don't -- split these",
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			o, c := splitComments(tc.input)
			assert.Equal(t, tc.output, o)
			assert.Equal(t, tc.comments, c)
		})
	}
}

func TestCommentTags(t *testing.T) {
	sql := `INSERT INTO "polls_question" ("question_text", "pub_date") VALUES ('What is this?', '2019-05-28T18:54:50.767481+00:00'::timestamptz) RETURNING "polls_question"."id" /*controller='index',db_driver='django.db.backends.postgresql',framework='django%3A2.2.1',route='%5Epolls/%24',traceparent='00-5bd66ef5095369c7b0d1f8f4bd33716a-c532cb4098ac3dd2-01',tracestate='congo%3Dt61rcWkgMzE%2Crojo%3D00f067aa0ba902b7'*/`

	q, comments := splitComments(sql)
	assert.Equal(t, "INSERT INTO \"polls_question\" (\"question_text\", \"pub_date\") VALUES ('What is this?', '2019-05-28T18:54:50.767481+00:00'::timestamptz) RETURNING \"polls_question\".\"id\"", q)
	assert.Equal(t, []string{"controller='index',db_driver='django.db.backends.postgresql',framework='django%3A2.2.1',route='%5Epolls/%24',traceparent='00-5bd66ef5095369c7b0d1f8f4bd33716a-c532cb4098ac3dd2-01',tracestate='congo%3Dt61rcWkgMzE%2Crojo%3D00f067aa0ba902b7'"}, comments)

	tags := parseCommentTags(comments)
	assert.Equal(t, []*pbvtgate.Query_Tag{
		{Key: "controller", Value: "index"},
		{Key: "db_driver", Value: "django.db.backends.postgresql"},
		{Key: "framework", Value: "django:2.2.1"},
		{Key: "route", Value: "^polls/$"},
		{Key: "traceparent", Value: "00-5bd66ef5095369c7b0d1f8f4bd33716a-c532cb4098ac3dd2-01"},
		{Key: "tracestate", Value: "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7"},
	}, tags)
}

func TestCommentTagsUgly(t *testing.T) {
	sql := ` /*one='1' , two='2' */ SELECT * FROM hello/*th%2dree= ' 3 ',    four='4\'s a great n\umber', five ='5\\cinco' ,, six='%foo'  */`

	q, comments := splitComments(sql)
	assert.Equal(t, "SELECT * FROM hello", q)
	assert.Equal(t, []string{"one='1' , two='2'", `th%2dree= ' 3 ',    four='4\'s a great n\umber', five ='5\\cinco' ,, six='%foo'`}, comments)

	tags := parseCommentTags(comments)
	assert.Equal(t, []*pbvtgate.Query_Tag{
		{Key: "one", Value: "1"},
		{Key: "two", Value: "2"},
		{Key: "th-ree", Value: " 3 "},
		{Key: "four", Value: "4's a great number"},
		{Key: "five", Value: `5\cinco`},
		{Key: "six", Value: "%foo"},
	}, tags)
}

func TestMoreCommentTags(t *testing.T) {
	testCases := []struct {
		comment string
		expect  []*pbvtgate.Query_Tag
	}{
		{
			"unterminated='", nil,
		},
		{
			"noequals", nil,
		},
		{
			"this is the sort of normal SQL comment that doesn't contain any tags: http://sample.com", nil,
		},
		{
			"almost=valid", nil, // single quotes are required
		},
		{
			"junk='after'the closing quotation mark is ignored",
			[]*pbvtgate.Query_Tag{
				{Key: "junk", Value: "after"},
			},
		},
		{
			"another normal SQL comment, with an = sign in it", nil,
		},
		{
			",internal-bonus='commas',,,are='skipped',", []*pbvtgate.Query_Tag{
				{Key: ",internal-bonus", Value: "commas"},
				{Key: "are", Value: "skipped"},
			},
		},
		{
			"commas='with', ,spaces='are-bad'", []*pbvtgate.Query_Tag{
				{Key: "commas", Value: "with"},
				{Key: ",spaces", Value: "are-bad"},
			},
		},
		{
			"space='before' ,comma='fine'", []*pbvtgate.Query_Tag{
				{Key: "space", Value: "before"},
				{Key: "comma", Value: "fine"},
			},
		},
		{
			"space='after', comma='fine'", []*pbvtgate.Query_Tag{
				{Key: "space", Value: "after"},
				{Key: "comma", Value: "fine"},
			},
		},
		{
			"the='first',error=,terminates='parsing'", []*pbvtgate.Query_Tag{
				{Key: "the", Value: "first"},
			},
		},
		{
			"empty='',values='',='and',='keys'", []*pbvtgate.Query_Tag{
				{Key: "empty", Value: ""},
				{Key: "values", Value: ""},
				{Key: "", Value: "and"},
				{Key: "", Value: "keys"},
			},
		},
		{
			`ends-with-backslash='foo\`, nil,
		},
		{
			`ends-with-escaped-backslash='foo\\`, nil,
		},
		{
			`ends-with-escaped-quote='foo\'`, nil,
		},
		{
			`escaped-unterminated-bare='\'`, nil,
		},
		{
			`escaped-unterminated-more='\'more`, nil,
		},
		{
			"", nil,
		},
		{
			`good-escape='foo\'bar\\baz',x='y'`, []*pbvtgate.Query_Tag{
				{Key: "good-escape", Value: `foo'bar\baz`},
				{Key: "x", Value: "y"},
			},
		},
		{
			`foo='\\\\\'\\\\\'\\'`, []*pbvtgate.Query_Tag{
				{Key: "foo", Value: `\\'\\'\`},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.comment, func(t *testing.T) {
			got := parseCommentTags([]string{tc.comment})
			assert.Equal(t, tc.expect, got)
		})
	}
}
