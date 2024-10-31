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

/*
	This file has been copied over from VTOrc package
*/

package sqlutils

import (
	"regexp"
	"strings"
)

func regexpSpaces(statement string) string {
	return strings.Replace(statement, " ", `[\s]+`, -1)
}

var (
	sqlite3IdentifyCreateIndexStatement = regexp.MustCompile(regexpSpaces(`^[\s]*create( unique|) index`))
	sqlite3IdentifyDropIndexStatement   = regexp.MustCompile(regexpSpaces(`^[\s]*drop index`))
	sqlite3IdentifyAlterTableStatement  = regexp.MustCompile(regexpSpaces(`^[\s]*alter table`))
)

func IsCreateIndex(statement string) bool {
	return sqlite3IdentifyCreateIndexStatement.MatchString(statement)
}

func IsDropIndex(statement string) bool {
	return sqlite3IdentifyDropIndexStatement.MatchString(statement)
}

func IsAlterTable(statement string) bool {
	return sqlite3IdentifyAlterTableStatement.MatchString(statement)
}
