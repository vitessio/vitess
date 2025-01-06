/*
Copyright 2024 The Vitess Authors.

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

package plan_tests

import (
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestE2ECases(t *testing.T) {
	e2eTestCaseFiles := []string{"select_cases.json", "filter_cases.json", "dml_cases.json"}
	mcmp, closer := start(t)
	defer closer()
	loadSampleData(t, mcmp)
	for _, fileName := range e2eTestCaseFiles {
		tests := readJSONTests(fileName)
		for _, test := range tests {
			mcmp.Run(test.Comment, func(mcmp *utils.MySQLCompare) {
				if test.SkipE2E {
					mcmp.AsT().Skip(test.Query)
				}
				mcmp.Exec(test.Query)
				pd := utils.ExecTrace(mcmp.AsT(), mcmp.VtConn, test.Query)
				verifyTestExpectations(mcmp.AsT(), pd, test)
				if mcmp.VtConn.IsClosed() {
					mcmp.AsT().Fatal("vtgate connection is closed")
				}
			})
		}
	}
}
