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

package sqlparser

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQuotedNamesDoNotSmuggleSQL covers names that the grammar accepts as a
// quoted string or a back-quoted identifier and that used to be stored as a
// plain Go string and re-emitted verbatim. Because vtgate ships
// sqlparser.String(stmt) to the tablets, any byte a client hid inside such a
// name became bare SQL text in the executed statement.
//
// Each case asserts the regenerated statement keeps the name as a single token,
// so re-parsing it yields the same statement rather than a different one.
func TestQuotedNamesDoNotSmuggleSQL(t *testing.T) {
	// smuggle is a payload that, emitted unquoted, turns each statement below
	// into a syntactically valid but entirely different one.
	const smuggle = `utf8mb4_bin union all select pw, 1 from users`

	testcases := []struct {
		name   string
		input  string
		output string
	}{{
		name:   "collate, string literal",
		input:  "select id from t where c = 'x' collate '" + smuggle + "'",
		output: "select id from t where c = 'x' collate `" + smuggle + "`",
	}, {
		name:   "collate, back-quoted identifier",
		input:  "select id from t where c = 'x' collate `" + smuggle + "`",
		output: "select id from t where c = 'x' collate `" + smuggle + "`",
	}, {
		name:   "convert using",
		input:  "select convert('x' using 'utf8mb4, add column q int') from t",
		output: "select convert('x' using `utf8mb4, add column q int`) from t",
	}, {
		name:   "char using",
		input:  "select char(0x41 using 'utf8mb4, add column q int') from t",
		output: "select char(0x41 using `utf8mb4, add column q int`) from t",
	}, {
		name:   "alter table convert to character set",
		input:  "alter table t convert to character set 'utf8mb4, add column q int'",
		output: "alter table t convert to character set `utf8mb4, add column q int`",
	}, {
		name:   "alter table convert to collate",
		input:  "alter table t convert to character set utf8mb4 collate `utf8mb4_bin, add column q int`",
		output: "alter table t convert to character set utf8mb4 collate `utf8mb4_bin, add column q int`",
	}, {
		name:   "table option charset",
		input:  "create table t (id int) charset 'utf8mb4, comment=\"x\"'",
		output: "create table t (\n\tid int\n) charset `utf8mb4, comment=\"x\"`",
	}, {
		name:   "table option collate",
		input:  "create table t (id int) collate `utf8mb4_bin, comment=\"x\"`",
		output: "create table t (\n\tid int\n) collate `utf8mb4_bin, comment=\"x\"`",
	}, {
		name:   "table option engine",
		input:  "create table t (id int) engine 'innodb, comment=\"x\"'",
		output: "create table t (\n\tid int\n) engine `innodb, comment=\"x\"`",
	}, {
		name:   "table option tablespace",
		input:  "create table t (id int) tablespace `ts, comment=\"x\"` storage disk",
		output: "create table t (\n\tid int\n) tablespace `ts, comment=\"x\"` storage disk",
	}, {
		// ALTER TABLE's options are written by TableOptions, not TableSpec.
		name:   "alter table option engine",
		input:  "alter table t engine 'innodb, add column q int'",
		output: "alter table t engine `innodb, add column q int`",
	}, {
		name:   "alter table option charset",
		input:  "alter table t charset 'utf8mb4, add column q int'",
		output: "alter table t charset `utf8mb4, add column q int`",
	}, {
		name:   "alter table option collate",
		input:  "alter table t collate 'utf8mb4_bin, add column q int'",
		output: "alter table t collate `utf8mb4_bin, add column q int`",
	}, {
		name:   "alter table option tablespace",
		input:  "alter table t tablespace `ts, add column q int`",
		output: "alter table t tablespace `ts, add column q int`",
	}, {
		name:   "column charset",
		input:  "create table t (a varchar(10) charset `utf8mb4, add column q int`)",
		output: "create table t (\n\ta varchar(10) character set `utf8mb4, add column q int`\n)",
	}, {
		name:   "column collate",
		input:  "create table t (a varchar(10) collate `utf8mb4_bin, add column q int`)",
		output: "create table t (\n\ta varchar(10) collate `utf8mb4_bin, add column q int`\n)",
	}, {
		name:   "database charset",
		input:  "create database d character set `utf8mb4, comment 'x'`",
		output: "create database d character set `utf8mb4, comment 'x'`",
	}, {
		name:   "database collate",
		input:  "create database d collate `utf8mb4_bin, comment 'x'`",
		output: "create database d collate `utf8mb4_bin, comment 'x'`",
	}, {
		// ALTER DATABASE has its own formatter, separate from CREATE DATABASE's.
		name:   "alter database charset",
		input:  "alter database d character set '" + smuggle + "'",
		output: "alter database d character set `" + smuggle + "`",
	}, {
		name:   "alter database collate",
		input:  "alter database d collate '" + smuggle + "'",
		output: "alter database d collate `" + smuggle + "`",
	}, {
		// ENCRYPTION is the one option here whose value is a string literal
		// rather than a name, so it is escaped as one. The unquoted spelling is
		// no longer parsed at all -- see TestEncryptionRequiresQuotedString.
		name:   "database encryption",
		input:  `create database d encryption 'Y, comment ''x'''`,
		output: `create database d encryption 'Y, comment \'x\''`,
	}, {
		name:   "index with parser",
		input:  "create table t (a int, key i (a) with parser `ngram, add column q int`)",
		output: "create table t (\n\ta int,\n\tkey i (a) with parser `ngram, add column q int`\n)",
	}, {
		name:   "index using",
		input:  "create table t (a int, key i (a) using `btree, add column q int`)",
		output: "create table t (\n\ta int,\n\tkey i (a) using `btree, add column q int`\n)",
	}, {
		name:   "partition engine",
		input:  "alter table t add partition (partition p0 values less than (1) engine 'InnoDB, comment=\"x\"')",
		output: "alter table t add partition (partition p0 values less than (1) engine `InnoDB, comment=\"x\"`)",
	}, {
		name:   "subpartition engine",
		input:  "alter table t add partition (partition p0 values less than (1) (subpartition s0 engine 'InnoDB, comment=\"x\"'))",
		output: "alter table t add partition (partition p0 values less than (1) (subpartition s0 engine `InnoDB, comment=\"x\"`))",
	}, {
		name:   "partition tablespace",
		input:  "alter table t add partition (partition p0 values less than (1) tablespace `ts, add column q int`)",
		output: "alter table t add partition (partition p0 values less than (1) tablespace `ts, add column q int`)",
	}, {
		name:   "convert cast charset",
		input:  "select convert('x', char(10) character set `utf8mb4, add column q int`) from t",
		output: "select convert('x', char(10) character set `utf8mb4, add column q int`) from t",
	}, {
		name:   "select into outfile charset",
		input:  "select id from t into outfile 'f' character set `utf8mb4 lines terminated by 'x'`",
		output: "select id from t into outfile 'f' character set `utf8mb4 lines terminated by 'x'`",
	}, {
		// A back-quote inside the name must be doubled, not terminate the quoting.
		name:   "back-quote inside name",
		input:  "select id from t where c = 'x' collate 'a`b'",
		output: "select id from t where c = 'x' collate `a``b`",
	}}

	parser := NewTestParser()
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.Parse(tc.input)
			require.NoError(t, err)

			got := String(stmt)
			assert.Equal(t, tc.output, got)
			// String goes through FormatFast; UnescapedString goes through
			// Format, and asks for identifiers to be left bare. Neither may
			// leave a name unquoted.
			assert.Equal(t, tc.output, UnescapedString(stmt), "Format and FormatFast must agree")

			// The regenerated statement must parse back to the same statement:
			// the name survives as one token and nothing new was introduced.
			reparsed, err := parser.Parse(got)
			require.NoError(t, err)
			assert.Equal(t, got, String(reparsed))
			assert.Equal(t, String(stmt), String(reparsed))
		})
	}
}

// TestBareNamesAreNotQuoted pins that ordinary charset, collation, engine,
// tablespace and index names keep their unquoted spelling, so the fix above
// does not churn generated DDL or break name lookups.
func TestBareNamesAreNotQuoted(t *testing.T) {
	testcases := []struct {
		input  string
		output string
	}{
		{input: "select id from t where c = 'x' collate utf8mb4_bin"},
		{
			input:  "select id from t where c = 'x' collate 'utf8mb4_bin'",
			output: "select id from t where c = 'x' collate utf8mb4_bin",
		},
		{input: "select convert('x' using utf8mb4) from t"},
		{input: "select char(0x41 using utf8mb4) from t"},
		{input: "alter table t convert to character set utf8mb4 collate utf8mb4_bin"},
		{input: "create table t (\n\tid int\n) charset utf8mb4,\n  collate utf8mb4_bin"},
		{input: "create table t (\n\tid int\n) engine InnoDB"},
		{input: "create table t (\n\tid int\n) tablespace ts storage disk"},
		{input: "create table t (\n\ta varchar(10) character set utf8mb4 collate utf8mb4_bin\n)"},
		{input: "create database d character set utf8mb4"},
		{input: "create table t (\n\ta int,\n\tkey i (a) using btree\n)"},
		{input: "create table t (\n\ta int,\n\tkey i (a) with parser ngram\n)"},
		{input: "create table t (\n\tid int\n) engine memory"},
		// `binary` is reserved, but the charset and collation rules accept it in
		// its own right, so these positions keep it bare.
		{input: "create table t (\n\tid int\n) charset binary"},
		{input: "create table t (\n\ta varchar(10) character set binary\n)"},
		{input: "create table t (\n\ta varchar(10) collate binary\n)"},
		{input: "create database d character set binary"},
		{input: "create database d collate binary"},
		{input: "alter table t add partition (partition p0 values less than (1) engine InnoDB)"},
	}

	parser := NewTestParser()
	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			stmt, err := parser.Parse(tc.input)
			require.NoError(t, err)
			want := tc.output
			if want == "" {
				want = tc.input
			}
			assert.Equal(t, want, String(stmt))
		})
	}
}

// TestEncryptionRequiresQuotedString guards this fix against widening what Vitess
// accepts. ENCRYPTION's value is written back out as a string literal, because
// that is the only spelling MySQL takes. If Vitess kept parsing the unquoted
// spelling it would now rewrite a statement MySQL rejects into one MySQL accepts,
// so it must not parse it: leaving the statement not fully parsed forwards the
// client's original text, and MySQL still rejects it.
func TestEncryptionRequiresQuotedString(t *testing.T) {
	parser := NewTestParser()

	stmt, err := parser.Parse("create database d encryption 'N'")
	require.NoError(t, err)
	require.True(t, stmt.(DBDDLStatement).IsFullyParsed())
	assert.Equal(t, "create database d encryption 'N'", String(stmt))

	for _, unquoted := range []string{
		"create database d encryption N",
		"create database d encryption `Y, comment 'x'`",
		"alter database d encryption Y",
	} {
		stmt, err := parser.Parse(unquoted)
		require.NoError(t, err)
		assert.False(t, stmt.(DBDDLStatement).IsFullyParsed(), unquoted)
	}
}

// TestExpressionCollateQuotesBinary covers the one charset position whose MySQL
// rule lacks a BINARY alternative. Vitess accepts `expr COLLATE binary`, but
// MySQL 8.0's `simple_expr COLLATE ident_or_text` rejects it with a syntax error
// and accepts it quoted, so the regenerated query must quote it or the tablet's
// MySQL will refuse a query the client wrote correctly.
func TestExpressionCollateQuotesBinary(t *testing.T) {
	parser := NewTestParser()
	for _, in := range []string{
		"select a from t where a = b collate 'binary'",
		"select a from t where a = b collate `binary`",
		"select a from t where a = b collate binary",
		"select a from t order by a collate binary",
	} {
		t.Run(in, func(t *testing.T) {
			stmt, err := parser.Parse(in)
			require.NoError(t, err)
			for _, out := range []string{String(stmt), UnescapedString(stmt), CanonicalString(stmt)} {
				assert.Contains(t, strings.ToLower(out), "collate `binary`")
				assert.NotContains(t, strings.ToLower(out), "collate binary")
			}
		})
	}
}

// TestSetNamesIgnoresCollation documents what SET NAMES and SET CHARACTER SET do
// with a collation: they accept one and discard it, so the session is left on the
// charset default. That is long-standing behaviour and none of the quoting work
// here changes it -- the test is here so that widening collate_opt, which these two
// share, is a deliberate choice rather than an accident.
//
// `binary` is accepted for the same reason every other collation name is: MySQL
// accepts it, and singling it out for a parse error would be arbitrary when the
// rest are accepted and ignored.
//
// An empty name is the exception: it is a parse error, as it is in MySQL; see
// TestEmptyNameIsRejected.
func TestSetNamesIgnoresCollation(t *testing.T) {
	parser := NewTestParser()
	testcases := []struct {
		input  string
		output string
	}{
		{input: "set names utf8mb4 collate utf8mb4_bin", output: "set names 'utf8mb4'"},
		{input: "set names utf8mb4 collate 'utf8mb4_bin'", output: "set names 'utf8mb4'"},
		{input: "set names utf8mb4 collate binary", output: "set names 'utf8mb4'"},
		{input: "set character set utf8mb4 collate utf8mb4_bin", output: "set charset 'utf8mb4'"},
	}
	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			stmt, err := parser.Parse(tc.input)
			require.NoError(t, err)
			// The collation is not represented, so it cannot be written back out.
			assert.Equal(t, tc.output, String(stmt))
		})
	}
}

// TestEmptyNameIsRejected covers the positions that used to store an empty
// charset or collation name pre-escaped, as a two-character string literal, and
// the ENGINE and partition TABLESPACE positions that accept a string. Now that
// the decoded name is stored, an empty one is indistinguishable from the clause
// being absent -- the formatters test `!= ""` to decide whether to write it -- so
// the clause would silently disappear or be mangled, and a statement MySQL
// rejects would become a valid, different one. There is no such name, so it is
// not parsed.
func TestEmptyNameIsRejected(t *testing.T) {
	parser := NewTestParser()

	// Not DDL, so the error reaches the client directly. The last three go
	// through the shared `charset` rule rather than charset_opt/collate_opt, so
	// they need their own guard -- without it the clause is dropped and the
	// statement becomes a different, valid one.
	for _, in := range []string{
		// SET NAMES and SET CHARACTER SET share collate_opt. They used to
		// accept an empty collation and discard it; MySQL rejects both.
		"set names utf8mb4 collate ''",
		"set character set utf8mb4 collate ''",
		"select * from t into outfile 'f' character set ''",
		"select convert('x', char(10) character set '') from t",
		"select 'x' collate '' from dual",
		"select convert(c using '') from t",
		"select char(65 using '') from t",
	} {
		t.Run(in, func(t *testing.T) {
			_, err := parser.Parse(in)
			assert.ErrorContains(t, err, "cannot be empty")
		})
	}

	// Database options are always written out, so an empty name cannot be
	// mistaken for an absent clause there; it is kept and written back as ''.
	for _, in := range []string{
		"create database d character set ''",
		"alter database d collate ''",
	} {
		t.Run(in, func(t *testing.T) {
			stmt, err := parser.Parse(in)
			require.NoError(t, err)
			require.True(t, isFullyParsed(stmt))
			assert.Equal(t, in, String(stmt))
		})
	}

	// DDL falls back to a partial parse, so the client's original text is
	// forwarded and MySQL rejects it. The strict path errors outright.
	for _, in := range []string{
		"create table t (a varchar(10) character set '')",
		"create table t (a varchar(10) collate '')",
		"create table t (a varchar(10) not null collate '')",
		"alter table t convert to character set utf8mb4 collate ''",
		// Table-option spellings, also on the shared `charset` rule. These used
		// to print as `charset ()`, which does not parse.
		"create table t (id int) charset ''",
		"create table t (id int) character set ''",
		"create table t (id int) collate ''",
		// ENGINE and partition TABLESPACE take table_alias, which accepts a
		// string. An empty engine used to print as `engine ()`, and an empty
		// partition tablespace was dropped from the output altogether.
		"create table t (id int) engine ''",
		"alter table t engine = ''",
		"alter table t add partition (partition p0 values less than (1) engine '')",
		"alter table t add partition (partition p0 values less than (1) (subpartition s0 engine ''))",
		"alter table t add partition (partition p0 values less than (1) tablespace '')",
		"alter table t add partition (partition p0 values less than (1) (subpartition s0 tablespace ''))",
		"create table t (id int) partition by range (id) (partition p0 values less than (1) tablespace '')",
	} {
		t.Run(in, func(t *testing.T) {
			stmt, err := parser.Parse(in)
			require.NoError(t, err)
			assert.False(t, stmt.(DDLStatement).IsFullyParsed(),
				"must not be fully parsed, or the clause would be silently dropped")

			_, err = parser.ParseStrictDDL(in)
			assert.ErrorContains(t, err, "cannot be empty")
		})
	}
}

// TestKeywordNamesAreQuoted pins the spelling a keyword name comes back in,
// position by position. TestEveryKeywordNameRoundTrips checks that every such
// name reads back; this pins that it does so back-quoted, rather than in some
// other spelling that happens to parse.
//
// Which keywords can stay bare depends on the production being written into:
//
//   - charset and collation take sql_id, and BINARY in its own right, so a
//     non-reserved keyword and `binary` both come back bare -- except before the
//     BINARY modifier, which the BINARY alternative does not take;
//   - ENGINE, TABLESPACE and index USING take sql_id or table_alias, which admit
//     a non-reserved keyword but have no BINARY alternative;
//   - WITH PARSER takes ci_identifier, which is an ID and nothing else, so every
//     keyword stays quoted there, non-reserved ones included.
func TestKeywordNamesAreQuoted(t *testing.T) {
	testcases := []struct {
		input  string
		output string
	}{{
		input:  "select 'x' collate 'select' from t",
		output: "select 'x' collate `select` from t",
	}, {
		input:  "create table t (a varchar(10) collate 'select')",
		output: "create table t (\n\ta varchar(10) collate `select`\n)",
	}, {
		input:  "create database d character set 'select'",
		output: "create database d character set `select`",
	}, {
		input:  "create table t (id int) engine 'select'",
		output: "create table t (\n\tid int\n) engine `select`",
	}, {
		input:  "create table t (id int) tablespace `select`",
		output: "create table t (\n\tid int\n) tablespace `select`",
	}, {
		input:  "create table t (a int, key i (a) with parser `select`)",
		output: "create table t (\n\ta int,\n\tkey i (a) with parser `select`\n)",
	}, {
		input:  "create table t (id int) engine 'binary'",
		output: "create table t (\n\tid int\n) engine `binary`",
	}, {
		input:  "create table t (id int) tablespace `binary`",
		output: "create table t (\n\tid int\n) tablespace `binary`",
	}, {
		input:  "create table t (id int, fulltext key (id) with parser `binary`)",
		output: "create table t (\n\tid int,\n\tfulltext key (id) with parser `binary`\n)",
	}, {
		input:  "create table t (id int, fulltext key (id) with parser `memory`)",
		output: "create table t (\n\tid int,\n\tfulltext key (id) with parser `memory`\n)",
	}, {
		input:  "select cast(a as char character set 'binary' binary) from t",
		output: "select cast(a as char character set `binary` binary) from t",
	}, {
		input:  "select convert(a, char(10) character set `binary` binary) from t",
		output: "select convert(a, char(10) character set `binary` binary) from t",
	}, {
		input:  "create table t (a varchar(10) character set 'binary' binary)",
		output: "create table t (\n\ta varchar(10) character set `binary` binary\n)",
	}}

	parser := NewTestParser()
	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			stmt, err := parser.Parse(tc.input)
			require.NoError(t, err)
			require.True(t, isFullyParsed(stmt))
			assert.Equal(t, tc.output, String(stmt))
			assert.Equal(t, tc.output, UnescapedString(stmt), "Format and FormatFast must agree")
		})
	}
}

// TestEncodeSQLName pins that a name is always written as exactly one token.
func TestEncodeSQLName(t *testing.T) {
	testcases := []struct {
		in, out string
	}{
		{in: "utf8mb4", out: "utf8mb4"},
		{in: "utf8mb4_0900_ai_ci", out: "utf8mb4_0900_ai_ci"},
		{in: "InnoDB", out: "InnoDB"},
		{in: "latin1", out: "latin1"},
		{in: "_x", out: "_x"},
		// `binary` is reserved, but the charset and collation rules accept it in
		// its own right, as MySQL's do, so it stays bare.
		{in: "binary", out: "binary"},
		{in: "BINARY", out: "BINARY"},
		// Non-reserved keywords are accepted where an identifier is, so also bare.
		{in: "memory", out: "memory"},
		{in: "ascii", out: "ascii"},
		// Any other reserved word must be quoted or it will not lex back.
		{in: "select", out: "`select`"},
		{in: "SELECT", out: "`SELECT`"},
		{in: "next", out: "`next`"},
		// So must a keyword that is neither reserved nor non-reserved, and a
		// charset introducer: neither lexes as something a name position takes.
		{in: "cast", out: "`cast`"},
		{in: "release", out: "`release`"},
		{in: "_utf8mb4", out: "`_utf8mb4`"},
		{in: "_BINARY", out: "`_BINARY`"},
		{in: "", out: "''"},
		{in: "1abc", out: "`1abc`"},
		{in: "a b", out: "`a b`"},
		{in: "a,b", out: "`a,b`"},
		{in: "a`b", out: "`a``b`"},
		{in: "a'b", out: "`a'b`"},
		{in: "üü", out: "`üü`"},
		{in: "a\nb", out: "`a\nb`"},
	}

	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			assert.Equal(t, tc.out, encodeSQLName(tc.in))
		})
	}
}

// TestPositionEncoders pins the encoders for positions that accept less than
// the charset rules do. Each defers to encodeSQLName except where noted.
func TestPositionEncoders(t *testing.T) {
	testcases := []struct {
		in         string
		object     string // ENGINE, TABLESPACE, USING, expression COLLATE
		identifier string // WITH PARSER
		binaryMod  string // a charset followed by the BINARY modifier
	}{
		{in: "utf8mb4", object: "utf8mb4", identifier: "utf8mb4", binaryMod: "utf8mb4"},
		{in: "ngram", object: "ngram", identifier: "ngram", binaryMod: "ngram"},
		// A non-reserved keyword is bare except where only an ID is taken.
		{in: "memory", object: "memory", identifier: "`memory`", binaryMod: "memory"},
		{in: "ascii", object: "ascii", identifier: "`ascii`", binaryMod: "ascii"},
		// `binary` is bare only where the BINARY alternative takes it.
		{in: "binary", object: "`binary`", identifier: "`binary`", binaryMod: "`binary`"},
		{in: "BINARY", object: "`BINARY`", identifier: "`BINARY`", binaryMod: "`BINARY`"},
		{in: "select", object: "`select`", identifier: "`select`", binaryMod: "`select`"},
		{in: "a`b", object: "`a``b`", identifier: "`a``b`", binaryMod: "`a``b`"},
	}

	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			assert.Equal(t, tc.object, encodeObjectName(tc.in))
			assert.Equal(t, tc.identifier, encodeIdentifierName(tc.in))
			assert.Equal(t, tc.binaryMod, encodeColumnCharsetName(ColumnCharset{Name: tc.in, Binary: true}))
			assert.Equal(t, encodeSQLName(tc.in), encodeColumnCharsetName(ColumnCharset{Name: tc.in}))
		})
	}
}

// TestQuotedNameIsSemanticallyEqual pins the payoff of holding the decoded name:
// a quoted charset or collation name now means the same thing as a bare one, the
// way MySQL treats it.
func TestQuotedNameIsSemanticallyEqual(t *testing.T) {
	parser := NewTestParser()
	pairs := [][2]string{
		{"select 'x' collate utf8mb4_bin", "select 'x' collate 'utf8mb4_bin'"},
		{"select 'x' collate utf8mb4_bin", "select 'x' collate `utf8mb4_bin`"},
		{"create table t (a varchar(10) collate utf8mb4_bin)", "create table t (a varchar(10) collate 'utf8mb4_bin')"},
		{"create table t (a varchar(10) charset utf8mb4)", "create table t (a varchar(10) charset 'utf8mb4')"},
		{"create table t (id int) charset utf8mb4", "create table t (id int) charset 'utf8mb4'"},
		{"create database d character set utf8mb4", "create database d character set 'utf8mb4'"},
		{"alter table t convert to character set utf8mb4 collate utf8mb4_bin", "alter table t convert to character set 'utf8mb4' collate 'utf8mb4_bin'"},
		// A reserved word as a name has no bare spelling that parses, so the two
		// quotings are compared against each other. Both have to come back
		// back-quoted, which is the spelling that reads again.
		{"select 'x' collate `select`", "select 'x' collate 'select'"},
		{"create table t (a varchar(10) collate `select`)", "create table t (a varchar(10) collate 'select')"},
		// `binary` is both a real collation name and a reserved keyword.
		{"select 'x' collate binary", "select 'x' collate 'binary'"},
		{"create table t (a varchar(10) collate binary)", "create table t (a varchar(10) collate 'binary')"},
		{"create database d character set binary", "create database d character set 'binary'"},
	}

	for _, pair := range pairs {
		t.Run(pair[1], func(t *testing.T) {
			bare, err := parser.Parse(pair[0])
			require.NoError(t, err)
			quoted, err := parser.Parse(pair[1])
			require.NoError(t, err)
			// Compare the regenerated statements rather than the ASTs: an
			// AliasedExpr keeps the client's original expression text verbatim,
			// so two spellings of one name are deliberately not AST-equal.
			assert.Equal(t, String(bare), String(quoted))
		})
	}
}

// TestEveryKeywordNameRoundTrips writes every word the tokenizer treats specially
// -- every entry in its keyword table, charset introducers such as `_utf8mb4`
// included -- into every position that takes a charset, collation, engine,
// tablespace, index type or parser name, and checks that the regenerated
// statement reads back as the same statement.
//
// Whether a name may be written bare depends on which token it lexes as, not
// merely on whether sql.y reserves it: these positions take an ID or a
// non-reserved keyword, so a keyword in neither list (`cast`, `release`) and an
// introducer have to be quoted just as a reserved word does.
func TestEveryKeywordNameRoundTrips(t *testing.T) {
	positions := []struct {
		name string
		sql  string
		// quote spells the name the way the position accepts it on the way
		// in. Most take a string literal; the ones that take only an
		// identifier get a back-quoted one.
		quote func(string) string
	}{
		{"expression collate", "select a from t where a = b collate %s", encodeSQLString},
		{"convert using", "select a from t where convert(a using %s) = b", encodeSQLString},
		{"char using", "select a from t where char(65 using %s) = b", encodeSQLString},
		{"cast character set", "select a from t where cast(a as char character set %s) = b", encodeSQLString},
		{"convert character set", "select a from t where convert(a, char(10) character set %s) = b", encodeSQLString},
		{"select into outfile character set", "select a from t into outfile 'f' character set %s", encodeSQLString},
		{"json_table column collate", "select * from json_table('[]', '$[*]' columns (a varchar(10) collate %s path '$')) as jt", encodeSQLString},
		{"column character set", "create table t (a varchar(10) character set %s)", encodeSQLString},
		{"column collate", "create table t (a varchar(10) collate %s)", encodeSQLString},
		{"create table charset", "create table t (a int) charset %s", encodeSQLString},
		{"create table collate", "create table t (a int) collate %s", encodeSQLString},
		{"create table engine", "create table t (a int) engine %s", encodeSQLString},
		{"create table tablespace", "create table t (a int) tablespace %s", backQuoteName},
		{"alter table charset", "alter table t charset %s", encodeSQLString},
		{"alter table collate", "alter table t collate %s", encodeSQLString},
		{"alter table engine", "alter table t engine %s", encodeSQLString},
		{"alter table tablespace", "alter table t tablespace %s", backQuoteName},
		{"alter table convert to character set", "alter table t convert to character set %s", encodeSQLString},
		{"alter table convert to collate", "alter table t convert to character set utf8mb4 collate %s", encodeSQLString},
		{"create database character set", "create database d character set %s", encodeSQLString},
		{"create database collate", "create database d collate %s", encodeSQLString},
		{"alter database character set", "alter database d character set %s", encodeSQLString},
		{"alter database collate", "alter database d collate %s", encodeSQLString},
		{"index using", "create table t (a int, key i (a) using %s)", backQuoteName},
		{"index with parser", "create table t (a text, fulltext key i (a) with parser %s)", backQuoteName},
		{"partition engine", "alter table t add partition (partition p0 values less than (1) engine %s)", encodeSQLString},
		{"partition tablespace", "alter table t add partition (partition p0 values less than (1) tablespace %s)", encodeSQLString},
		{"subpartition engine", "alter table t add partition (partition p0 values less than (1) (subpartition s0 engine %s))", encodeSQLString},
		{"subpartition tablespace", "alter table t add partition (partition p0 values less than (1) (subpartition s0 tablespace %s))", encodeSQLString},
		{"order by collate", "select a from t order by a collate %s", encodeSQLString},
		{"json_value returning character set", "select a from t where json_value(a, '$' returning char character set %s) = b", encodeSQLString},
		{"select into outfile s3 character set", "select a from t into outfile s3 'f' character set %s", encodeSQLString},
		{"enum character set", "create table t (a enum('x') character set %s)", encodeSQLString},
		{"column collate after attributes", "create table t (a varchar(10) not null collate %s)", backQuoteName},
		{"generated column collate", "create table t (a varchar(10) collate %s as (b))", encodeSQLString},
		{"alter table add column character set", "alter table t add column a varchar(10) character set %s", encodeSQLString},
		{"alter table modify column collate", "alter table t modify column a varchar(10) collate %s", encodeSQLString},
		{"create table tablespace storage", "create table t (a int) tablespace %s storage disk", backQuoteName},
		{"alter table engine with actions", "alter table t add column b int, engine %s", encodeSQLString},
		{"alter table add index using", "alter table t add index i (a) using %s", backQuoteName},
		{"create index using", "create index i using %s on t (a)", backQuoteName},
		{"alter table add fulltext with parser", "alter table t add fulltext index i (a) with parser %s", backQuoteName},
		// A charset followed by the BINARY modifier: the charset rule's own
		// BINARY alternative takes no modifier after it.
		{"cast character set binary", "select a from t where cast(a as char character set %s binary) = b", encodeSQLString},
		{"convert character set binary", "select a from t where convert(a, char(10) character set %s binary) = b", encodeSQLString},
		{"column character set binary", "create table t (a varchar(10) character set %s binary)", encodeSQLString},
	}

	var names []string
	introducers := 0
	for _, kw := range keywords {
		names = append(names, kw.name, strings.ToUpper(kw.name))
		if strings.HasPrefix(kw.name, "_") {
			introducers++
		}
	}
	// A canary, so that this cannot pass by iterating over nothing.
	require.Greater(t, len(keywords), 500)
	require.Greater(t, introducers, 40, "charset introducers are expected in the keyword table")

	parser := NewTestParser()
	for _, pos := range positions {
		t.Run(pos.name, func(t *testing.T) {
			var failures []string
			for _, name := range names {
				in := fmt.Sprintf(pos.sql, pos.quote(name))
				stmt, err := parser.Parse(in)
				require.NoError(t, err, in)
				require.True(t, isFullyParsed(stmt), "precondition: %q is fully parsed", in)

				// String must read back as an equal AST. CanonicalString
				// upper-cases the table option names the AST keeps, so its
				// output is held to reading back as itself instead.
				for _, format := range []struct {
					print func(SQLNode) string
					same  func(a, b Statement) bool
				}{
					{String, Equals.Statement},
					{CanonicalString, func(a, b Statement) bool { return CanonicalString(a) == CanonicalString(b) }},
				} {
					out := format.print(stmt)
					reparsed, err := parser.Parse(out)
					switch {
					case err != nil:
						failures = append(failures, fmt.Sprintf("%q: %q does not parse: %v", name, out, err))
					case !isFullyParsed(reparsed):
						failures = append(failures, fmt.Sprintf("%q: %q is not fully parsed", name, out))
					case !format.same(stmt, reparsed):
						failures = append(failures, fmt.Sprintf("%q: %q reads back as %q", name, out, format.print(reparsed)))
					}
				}
			}
			assert.Empty(t, failures, "%d of %d names do not round-trip", len(failures), 2*len(names))
		})
	}
}

// isFullyParsed reports whether stmt is a statement the parser understood in
// full, rather than DDL it fell back to forwarding verbatim.
func isFullyParsed(stmt Statement) bool {
	switch stmt := stmt.(type) {
	case DDLStatement:
		return stmt.IsFullyParsed()
	case DBDDLStatement:
		return stmt.IsFullyParsed()
	}
	return true
}
