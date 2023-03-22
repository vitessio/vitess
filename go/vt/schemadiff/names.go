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
	"fmt"
	"regexp"
	"strings"
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
	if strings.HasPrefix(constraintName, fmt.Sprintf("%s_chk_", tableName)) {
		return constraintName[len(tableName)+1:]
	}
	if strings.HasPrefix(constraintName, fmt.Sprintf("%s_fk_", tableName)) {
		return constraintName[len(tableName)+1:]
	}
	if submatch := constraintVitessNameRegexp.FindStringSubmatch(constraintName); len(submatch) > 0 {
		return submatch[1]
	}

	return constraintName
}
