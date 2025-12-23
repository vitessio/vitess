/*
Copyright 2022 The Vitess Authors.

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

package schemadiff

import (
	"regexp"
	"strconv"
	"strings"

	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/sqlparser"
)

// constraint name examples:
// - check1
// - check1_db9d15e218979970c23bfc13a
// - isnegative_2dbf8418af7250d783beb2083
// - employee_id_af3d858eb99b87d6419133d5c
// where:
// - db9d15e218979970c23bfc13a is a deterministic vitess-generates sum (128 bit encoded with base36, 25 characters)
// - check1, isnegative, employee_id -- are the original constraints names
var constraintVitessNameRegexp = regexp.MustCompile(`^(.*?)(_([0-9a-z]{25}))?$`)

// ExtractConstraintOriginalName extracts what used to be the constraint name
// before schemadiff/vitess generated a replacement name.
// e.g. input: "check1_7no794p1x6zw6je1gfqmt7bca", output: "check1"
func ExtractConstraintOriginalName(tableName string, constraintName string) string {
	if strings.HasPrefix(constraintName, tableName+"_chk_") {
		return constraintName[len(tableName)+1:]
	}
	if strings.HasPrefix(constraintName, tableName+"_ibfk_") {
		return constraintName[len(tableName)+1:]
	}
	if submatch := constraintVitessNameRegexp.FindStringSubmatch(constraintName); len(submatch) > 0 {
		return submatch[1]
	}

	return constraintName
}

// newConstraintName generates a new, unique name for a constraint. Our problem is that a MySQL
// constraint's name is unique in the schema (!). And so as we duplicate the original table, we must
// create completely new names for all constraints.
// Moreover, we really want this name to be consistent across all shards. We therefore use a deterministic
// UUIDv5 (SHA) function over the migration UUID, table name, and constraint's _contents_.
// We _also_ include the original constraint name as prefix, as room allows
// for example, if the original constraint name is "check_1",
// we might generate "check_1_cps1okb4uafunfqusi2lp22u3".
// If we then again migrate a table whose constraint name is "check_1_cps1okb4uafunfqusi2lp22u3	" we
// get for example "check_1_19l09s37kbhj4axnzmi10e18k" (hash changes, and we still try to preserve original name)
//
// Furthermore, per bug report https://bugs.mysql.com/bug.php?id=107772, if the user doesn't provide a name for
// their CHECK constraint, then MySQL picks a name in this format <tablename>_chk_<number>.
// Example: sometable_chk_1
// Next, when MySQL is asked to RENAME TABLE and sees a constraint with this format, it attempts to rename
// the constraint with the new table's name. This is problematic for Vitess, because we often rename tables to
// very long names, such as _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_.
// As we rename the constraint to e.g. `sometable_chk_1_cps1okb4uafunfqusi2lp22u3`, this makes MySQL want to
// call the new constraint something like _vt_hld_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_chk_1_cps1okb4uafunfqusi2lp22u3,
// which exceeds the 64 character limit for table names. Long story short, we also trim down <tablename> if the constraint seems
// to be auto-generated.
func newConstraintName(tableName string, baseUUID string, constraintDefinition *sqlparser.ConstraintDefinition, hashExists map[string]bool, seed string, oldName string) string {
	constraintType := GetConstraintType(constraintDefinition.Details)

	constraintIndicator := constraintIndicatorMap[int(constraintType)]
	oldName = ExtractConstraintOriginalName(tableName, oldName)
	hash := textutil.UUIDv5Base36(baseUUID, tableName, seed)
	for i := 1; hashExists[hash]; i++ {
		hash = textutil.UUIDv5Base36(baseUUID, tableName, seed, strconv.Itoa(i))
	}
	hashExists[hash] = true
	suffix := "_" + hash
	maxAllowedNameLength := maxConstraintNameLength - len(suffix)
	newName := oldName
	if newName == "" {
		newName = constraintIndicator // start with something that looks consistent with MySQL's naming
	}
	if len(newName) > maxAllowedNameLength {
		newName = newName[0:maxAllowedNameLength]
	}
	newName = newName + suffix
	return newName
}
