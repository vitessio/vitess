/*
Copyright 2019 The Vitess Authors.

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

package filecustomrule

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
)

var customRule1 = `[
				{
					"Name": "r1",
					"Description": "disallow bindvar 'asdfg'",
					"BindVarConds":[{
						"Name": "asdfg",
						"OnAbsent": false,
						"Operator": ""
					}]
				}
			]`

func TestFileCustomRule(t *testing.T) {
	tqsc := tabletservermock.NewController()

	var qrs *rules.Rules
	rulepath := path.Join(os.TempDir(), ".customrule.json")
	// Set r1 and try to get it back
	err := ioutil.WriteFile(rulepath, []byte(customRule1), os.FileMode(0644))
	if err != nil {
		t.Fatalf("Cannot write r1 to rule file %s, err=%v", rulepath, err)
	}

	fcr := NewFileCustomRule()
	// Let FileCustomRule to build rule from the local file
	err = fcr.Open(tqsc, rulepath)
	if err != nil {
		t.Fatalf("Cannot open file custom rule service, err=%v", err)
	}
	// Fetch query rules we built to verify correctness
	qrs, _, err = fcr.GetRules()
	if err != nil {
		t.Fatalf("GetRules returns error: %v", err)
	}
	qr := qrs.Find("r1")
	if qr == nil {
		t.Fatalf("Expect custom rule r1 to be found, but got nothing, qrs=%v", qrs)
	}
}
