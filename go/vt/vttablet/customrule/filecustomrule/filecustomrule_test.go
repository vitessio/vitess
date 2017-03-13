// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package filecustomrule

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/rules"
	"github.com/youtube/vitess/go/vt/vttablet/tabletservermock"
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
