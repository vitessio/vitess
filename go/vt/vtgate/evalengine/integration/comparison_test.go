/*
Copyright 2021 The Vitess Authors.

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

package integration

import (
	"fmt"
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql"
)

func perm(a []string, f func([]string)) {
	perm1(a, f, 0)
}

func perm1(a []string, f func([]string), i int) {
	if i > len(a) {
		f(a)
		return
	}
	perm1(a, f, i+1)
	for j := i + 1; j < len(a); j++ {
		a[i], a[j] = a[j], a[i]
		perm1(a, f, i+1)
		a[i], a[j] = a[j], a[i]
	}
}

func compareRemoteQuery(t *testing.T, conn *mysql.Conn, query string) {
	t.Helper()

	local, _, localErr := safeEvaluate(query)
	if localErr != nil {
		t.Errorf("local failure: %v", localErr)
		return
	}
	remote, remoteErr := conn.ExecuteFetch(query, 1, false)
	if remoteErr != nil {
		t.Errorf("remote failure: %v", remoteErr)
		return
	}

	if local.Value().String() != remote.Rows[0][0].String() {
		t.Errorf("mismatch for query %q: local=%v, remote=%v", query, local.Value().String(), remote.Rows[0][0].String())
	}
}

func TestAllComparisons(t *testing.T) {
	var elems = []string{"NULL", "-1", "0", "1"}
	var operators = []string{"=", "!=", "<=>", "<", "<=", ">", ">="}

	var tuples []string
	perm(elems, func(t []string) {
		tuples = append(tuples, "("+strings.Join(t, ", ")+")")
	})

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, op := range operators {
		t.Run(op, func(t *testing.T) {
			for i := 0; i < len(tuples); i++ {
				for j := 0; j < len(tuples); j++ {
					query := fmt.Sprintf("SELECT %s %s %s", tuples[i], op, tuples[j])
					compareRemoteQuery(t, conn, query)
				}
			}
		})
	}
}

func TestAllIsStatements(t *testing.T) {
	var left = []string{
		"NULL", "TRUE", "FALSE",
		`1`, `0`, `1.0`, `0.0`, `-1`, `666`,
		`"1"`, `"0"`, `"1.0"`, `"0.0"`, `"-1"`, `"666"`,
		`"POTATO"`, `""`, `" "`, `"    "`,
	}
	var right = []string{
		"NULL",
		"NOT NULL",
		"TRUE",
		"NOT TRUE",
		"FALSE",
		"NOT FALSE",
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, l := range left {
		for _, r := range right {
			query := fmt.Sprintf("SELECT %s IS %s", l, r)
			compareRemoteQuery(t, conn, query)
		}
	}
}
