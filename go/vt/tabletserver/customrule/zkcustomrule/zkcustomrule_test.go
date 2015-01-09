// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkcustomrule

import (
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/zk"
	"github.com/youtube/vitess/go/zk/fakezk"
	"launchpad.net/gozk/zookeeper"
)

var customRule1 string = `[
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

var customRule2 string = `[
                                {
					"Name": "r2",
					"Description": "disallow insert on table test",
					"TableNames" : ["test"],
					"Query" : "(insert)|(INSERT)"
				}
			]`
var conn zk.Conn

func setUpFakeZk(t *testing.T) {
	conn = fakezk.NewConn()
	conn.Create("/zk", "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	conn.Create("/zk/fake", "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	conn.Create("/zk/fake/customrules", "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	conn.Create("/zk/fake/customrules/testrules", "customrule1", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	conn.Set("/zk/fake/customrules/testrules", customRule1, -1)
}

func TestZkCustomRule(t *testing.T) {
	setUpFakeZk(t)
	zkcr := NewZkCustomRule(conn)
	err := zkcr.Open("/zk/fake/customrules/testrules")
	if err != nil {
		t.Fatalf("Cannot open zookeeper custom rule service, err=%v", err)
	}

	var qrs *tabletserver.QueryRules
	// Test if we can successfully fetch the original rule (test GetRules)
	qrs, _, err = zkcr.GetRules()
	if err != nil {
		t.Fatalf("GetRules of ZkCustomRule should always return nil error, but we receive %v", err)
	}
	qr := qrs.Find("r1")
	if qr == nil {
		t.Fatalf("Expect custom rule r1 to be found, but got nothing, qrs=%v", qrs)
	}

	// Test updating rules
	conn.Set("/zk/fake/customrules/testrules", customRule2, -1)
	<-time.After(time.Second) //Wait for the polling thread to respond
	qrs, _, err = zkcr.GetRules()
	if err != nil {
		t.Fatalf("GetRules of ZkCustomRule should always return nil error, but we receive %v", err)
	}
	qr = qrs.Find("r2")
	if qr == nil {
		t.Fatalf("Expect custom rule r2 to be found, but got nothing, qrs=%v", qrs)
	}
	qr = qrs.Find("r1")
	if qr != nil {
		t.Fatalf("Custom rule r1 should not be found after r2 is set")
	}

	// Test rule path removal
	conn.Delete("/zk/fake/customrules/testrules", -1)
	<-time.After(time.Second)
	qrs, _, err = zkcr.GetRules()
	if err != nil {
		t.Fatalf("GetRules of ZkCustomRule should always return nil error, but we receive %v", err)
	}
	if reflect.DeepEqual(qrs, tabletserver.NewQueryRules()) {
		t.Fatalf("Expect empty rule at this point")
	}

	// Test rule path revival
	conn.Create("/zk/fake/customrules/testrules", "customrule2", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	conn.Set("/zk/fake/customrules/testrules", customRule2, -1)
	<-time.After(time.Second) //Wait for the polling thread to respond
	qrs, _, err = zkcr.GetRules()
	if err != nil {
		t.Fatalf("GetRules of ZkCustomRule should always return nil error, but we receive %v", err)
	}
	qr = qrs.Find("r2")
	if qr == nil {
		t.Fatalf("Expect custom rule r2 to be found, but got nothing, qrs=%v", qrs)
	}

	zkcr.Close()
}
