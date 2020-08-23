/*
   Copyright 2017 GitHub Inc.

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

// What's this about?
// This is a brute-force regular-expression based conversion from MySQL syntax to sqlite3 syntax.
// It is NOT meant to be a general purpose solution and is only expected & confirmed to run on
// queries issued by orchestrator. There are known limitations to this design.
// It's not even pretty.
// In fact...
// Well, it gets the job done at this time. Call it debt.

package sqlutils

import (
	"regexp"
)

var sqlite3CreateTableConversions = []regexpMap{
	rmap(`(?i) (character set|charset) [\S]+`, ``),
	rmap(`(?i)int unsigned`, `int`),
	rmap(`(?i)int[\s]*[(][\s]*([0-9]+)[\s]*[)] unsigned`, `int`),
	rmap(`(?i)engine[\s]*=[\s]*(innodb|myisam|ndb|memory|tokudb)`, ``),
	rmap(`(?i)DEFAULT CHARSET[\s]*=[\s]*[\S]+`, ``),
	rmap(`(?i)[\S]*int( not null|) auto_increment`, `integer`),
	rmap(`(?i)comment '[^']*'`, ``),
	rmap(`(?i)after [\S]+`, ``),
	rmap(`(?i)alter table ([\S]+) add (index|key) ([\S]+) (.+)`, `create index ${3}_${1} on $1 $4`),
	rmap(`(?i)alter table ([\S]+) add unique (index|key) ([\S]+) (.+)`, `create unique index ${3}_${1} on $1 $4`),
	rmap(`(?i)([\S]+) enum[\s]*([(].*?[)])`, `$1 text check($1 in $2)`),
	rmap(`(?i)([\s\S]+[/][*] sqlite3-skip [*][/][\s\S]+)`, ``),
	rmap(`(?i)timestamp default current_timestamp`, `timestamp default ('')`),
	rmap(`(?i)timestamp not null default current_timestamp`, `timestamp not null default ('')`),

	rmap(`(?i)add column (.*int) not null[\s]*$`, `add column $1 not null default 0`),
	rmap(`(?i)add column (.* text) not null[\s]*$`, `add column $1 not null default ''`),
	rmap(`(?i)add column (.* varchar.*) not null[\s]*$`, `add column $1 not null default ''`),
}

var sqlite3InsertConversions = []regexpMap{
	rmap(`(?i)insert ignore ([\s\S]+) on duplicate key update [\s\S]+`, `insert or ignore $1`),
	rmap(`(?i)insert ignore`, `insert or ignore`),
	rmap(`(?i)now[(][)]`, `datetime('now')`),
	rmap(`(?i)insert into ([\s\S]+) on duplicate key update [\s\S]+`, `replace into $1`),
}

var sqlite3GeneralConversions = []regexpMap{
	rmap(`(?i)now[(][)][\s]*[-][\s]*interval [?] ([\w]+)`, `datetime('now', printf('-%d $1', ?))`),
	rmap(`(?i)now[(][)][\s]*[+][\s]*interval [?] ([\w]+)`, `datetime('now', printf('+%d $1', ?))`),
	rmap(`(?i)now[(][)][\s]*[-][\s]*interval ([0-9.]+) ([\w]+)`, `datetime('now', '-${1} $2')`),
	rmap(`(?i)now[(][)][\s]*[+][\s]*interval ([0-9.]+) ([\w]+)`, `datetime('now', '+${1} $2')`),

	rmap(`(?i)[=<>\s]([\S]+[.][\S]+)[\s]*[-][\s]*interval [?] ([\w]+)`, ` datetime($1, printf('-%d $2', ?))`),
	rmap(`(?i)[=<>\s]([\S]+[.][\S]+)[\s]*[+][\s]*interval [?] ([\w]+)`, ` datetime($1, printf('+%d $2', ?))`),

	rmap(`(?i)unix_timestamp[(][)]`, `strftime('%s', 'now')`),
	rmap(`(?i)unix_timestamp[(]([^)]+)[)]`, `strftime('%s', $1)`),
	rmap(`(?i)now[(][)]`, `datetime('now')`),
	rmap(`(?i)cast[(][\s]*([\S]+) as signed[\s]*[)]`, `cast($1 as integer)`),

	rmap(`(?i)\bconcat[(][\s]*([^,)]+)[\s]*,[\s]*([^,)]+)[\s]*[)]`, `($1 || $2)`),
	rmap(`(?i)\bconcat[(][\s]*([^,)]+)[\s]*,[\s]*([^,)]+)[\s]*,[\s]*([^,)]+)[\s]*[)]`, `($1 || $2 || $3)`),

	rmap(`(?i) rlike `, ` like `),

	rmap(`(?i)create index([\s\S]+)[(][\s]*[0-9]+[\s]*[)]([\s\S]+)`, `create index ${1}${2}`),
	rmap(`(?i)drop index ([\S]+) on ([\S]+)`, `drop index if exists $1`),
}

var (
	sqlite3IdentifyCreateTableStatement = regexp.MustCompile(regexpSpaces(`(?i)^[\s]*create table`))
	sqlite3IdentifyCreateIndexStatement = regexp.MustCompile(regexpSpaces(`(?i)^[\s]*create( unique|) index`))
	sqlite3IdentifyDropIndexStatement   = regexp.MustCompile(regexpSpaces(`(?i)^[\s]*drop index`))
	sqlite3IdentifyAlterTableStatement  = regexp.MustCompile(regexpSpaces(`(?i)^[\s]*alter table`))
	sqlite3IdentifyInsertStatement      = regexp.MustCompile(regexpSpaces(`(?i)^[\s]*(insert|replace)`))
)

func IsInsert(statement string) bool {
	return sqlite3IdentifyInsertStatement.MatchString(statement)
}

func IsCreateTable(statement string) bool {
	return sqlite3IdentifyCreateTableStatement.MatchString(statement)
}

func IsCreateIndex(statement string) bool {
	return sqlite3IdentifyCreateIndexStatement.MatchString(statement)
}

func IsDropIndex(statement string) bool {
	return sqlite3IdentifyDropIndexStatement.MatchString(statement)
}

func IsAlterTable(statement string) bool {
	return sqlite3IdentifyAlterTableStatement.MatchString(statement)
}

func ToSqlite3CreateTable(statement string) string {
	return applyConversions(statement, sqlite3CreateTableConversions)
}

func ToSqlite3Insert(statement string) string {
	return applyConversions(statement, sqlite3InsertConversions)
}

func ToSqlite3Dialect(statement string) (translated string) {
	if IsCreateTable(statement) {
		return ToSqlite3CreateTable(statement)
	}
	if IsAlterTable(statement) {
		return ToSqlite3CreateTable(statement)
	}
	statement = applyConversions(statement, sqlite3GeneralConversions)
	if IsInsert(statement) {
		return ToSqlite3Insert(statement)
	}
	return statement
}
