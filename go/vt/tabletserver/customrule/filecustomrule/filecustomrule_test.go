// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package filecustomrule

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/tabletserver"
)

var customRule1 = `[
				{
					"Name": "r1",
					"Description": "disallow bindvar 'asdfg'",
					"BindVarConds":[{
						"Name": "asdfg",
						"OnAbsent": false,
						"Operator": "NOOP"
					}]
				}
			]`

var customRule2 = `[
                                {
					"Name": "r2",
					"Description": "disallow insert on table test",
					"TableNames" : ["test"],
					"Query" : "(insert)|(INSERT)"
				}
			]`

func TestFileCustomRule(t *testing.T) {
	var qrs *tabletserver.QueryRules
	rulepath := path.Join(os.TempDir(), ".customrule.json")
	err := ioutil.WriteFile(rulepath, []byte("[]"), os.FileMode(0644))
	if err != nil {
		t.Fatalf("Cannot write to rule file %s, err=%v", rulepath, err)
	}
	fcr := NewFileCustomRule(MinFilePollingInterval)
	err = fcr.Open(rulepath)
	if err != nil {
		t.Fatalf("Cannot open file custom rule service, err=%v", err)
	}

	// Set r1 and try to get it back
	err = ioutil.WriteFile(rulepath, []byte(customRule1), os.FileMode(0644))
	if err != nil {
		t.Fatalf("Cannot write r1 to rule file %s, err=%v", rulepath, err)
	}
	<-time.After(time.Second * MinFilePollingInterval * 2)
	qrs, _, err = fcr.GetRules()
	if err != nil {
		t.Fatalf("GetRules returns error: %v", err)
	}
	qr := qrs.Find("r1")
	if qr == nil {
		t.Fatalf("Expect custom rule r1 to be found, but got nothing, qrs=%v", qrs)
	}

	// Set r2 and try to get it back
	err = ioutil.WriteFile(rulepath, []byte(customRule2), os.FileMode(0644))
	if err != nil {
		t.Fatalf("Cannot write r2 to rule file %s, err=%v", rulepath, err)
	}
	<-time.After(time.Second * MinFilePollingInterval * 2)
	qrs, _, err = fcr.GetRules()
	if err != nil {
		t.Fatalf("GetRules returns error: %v", err)
	}
	qr = qrs.Find("r2")
	if qr == nil {
		t.Fatalf("Expect custom rule r2 to be found, but got nothing, qrs=%v", qrs)
	}
	qr = qrs.Find("r1")
	if qr != nil {
		t.Fatalf("Custom rule r1 should not be found after r2 is set")
	}

	// Test Error handling by removing the file
	os.Remove(rulepath)
	<-time.After(time.Second * MinFilePollingInterval * 2)
	qrs, _, err = fcr.GetRules()
	if err != nil {
		t.Fatalf("GetRules returns error: %v", err)
	}
	qr = qrs.Find("r2")
	if qr == nil {
		t.Fatalf("Expect custom rule r2 to be found even after rule file removal, but got nothing, qrs=%v", qrs)
	}
}
