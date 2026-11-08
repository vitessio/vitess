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

package mysqltest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReadFile tests that a mysqltest file is split into its statements the
// way mysqltest splits it, with the expected errors, the session state and the
// connection character set of each statement.
func TestReadFile(t *testing.T) {
	content := `# A comment.
create table t1 (a int); create table t2 (b int);
--error ER_PARSE_ERROR
create table t3 (a int
  default ';');
let $x = 1;
select $x;
delimiter //;
create procedure p() begin select 1; end//
delimiter ;//
set sql_mode = 'ANSI_QUOTES';
create table t4 (a int);
set sql_mode = default;
set names gb18030;
create table t5 (a int);
set names utf8mb4;
--character_set utf16
create table t6 (a int);
character_set latin1;
create table t7 (a int);
set names 'UTF8MB4';
create table t8 (a int);
`
	path := filepath.Join(t.TempDir(), "example.test")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	statements, err := ReadFile(path, "t/example.test")
	require.NoError(t, err)
	expected := []Statement{
		{Line: 2, SQL: "create table t1 (a int)"},
		{Line: 2, SQL: "create table t2 (b int)"},
		{Line: 4, SQL: "create table t3 (a int\n  default ';')", ExpectedErrors: "ER_PARSE_ERROR"},
		{Line: 9, SQL: "create procedure p() begin select 1; end"},
		{Line: 11, SQL: "set sql_mode = 'ANSI_QUOTES'", UnsupportedSession: true},
		{Line: 12, SQL: "create table t4 (a int)", UnsupportedSession: true},
		{Line: 13, SQL: "set sql_mode = default"},
		{Line: 14, SQL: "set names gb18030", UnsupportedSession: true, Charset: "gb18030"},
		{Line: 15, SQL: "create table t5 (a int)", UnsupportedSession: true, Charset: "gb18030"},
		{Line: 16, SQL: "set names utf8mb4", Charset: "utf8mb4"},
		{Line: 18, SQL: "create table t6 (a int)", UnsupportedSession: true, Charset: "utf16"},
		{Line: 20, SQL: "create table t7 (a int)", Charset: "latin1"},
		{Line: 21, SQL: "set names 'UTF8MB4'", Charset: "utf8mb4"},
		{Line: 22, SQL: "create table t8 (a int)", Charset: "utf8mb4"},
	}
	for i := range expected {
		expected[i].File = "t/example.test"
	}
	assert.Equal(t, expected, statements)
}

// TestReadFileMissing tests that a file that cannot be read is an error.
func TestReadFileMissing(t *testing.T) {
	_, err := ReadFile(filepath.Join(t.TempDir(), "missing.test"), "missing.test")
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestIsParseError(t *testing.T) {
	assert.True(t, IsParseError("ER_PARSE_ERROR"))
	assert.True(t, IsParseError("1064, ER_PARSE_ERROR"))
	// A statement that may fail with another error may parse.
	assert.False(t, IsParseError("ER_PARSE_ERROR, ER_NO_SUCH_TABLE"))
	assert.False(t, IsParseError(""))
}

func TestIsDDL(t *testing.T) {
	assert.True(t, IsDDL("create temporary table t (a int)"))
	assert.True(t, IsDDL("ALTER /* comment */ TABLE t ADD b int"))
	assert.True(t, IsDDL("create unique index i on t (a)"))
	assert.True(t, IsDDL("alter schema d charset latin1"))
	assert.False(t, IsDDL("create view v as select 1"))
	assert.False(t, IsDDL("select 1"))
}
