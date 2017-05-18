/*
Copyright 2017 Google Inc.

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

package zkcustomrule

import (
	"reflect"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/testfiles"
	"github.com/youtube/vitess/go/vt/topo/zk2topo"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/rules"
	"github.com/youtube/vitess/go/vt/vttablet/tabletservermock"
	"github.com/youtube/vitess/go/zk/zkctl"
)

var customRule1 = `
[
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

var customRule2 = `
[
  {
    "Name": "r2",
    "Description": "disallow insert on table test",
    "TableNames" : ["test"],
    "Query" : "(insert)|(INSERT)"
  }
]`

func TestZkCustomRule(t *testing.T) {
	// Start a real single ZK daemon, and close it after all tests are done.
	zkd, serverAddr := zkctl.StartLocalZk(testfiles.GoVtTabletserverCustomruleZkcustomruleZkID, testfiles.GoVtTabletserverCustomruleZkcustomrulePort)
	defer zkd.Teardown()

	// Create fake file.
	serverPath := "/zk/fake/customrules/testrules"
	ctx := context.Background()
	conn := zk2topo.Connect(serverAddr)
	defer conn.Close()
	if _, err := zk2topo.CreateRecursive(ctx, conn, serverPath, []byte(customRule1), 0, zk.WorldACL(zk2topo.PermFile), 3); err != nil {
		t.Fatalf("CreateRecursive failed: %v", err)
	}

	// Start a mock tabletserver.
	tqsc := tabletservermock.NewController()

	// Setup the ZkCustomRule
	zkcr := NewZkCustomRule(serverAddr, serverPath)
	err := zkcr.Start(tqsc)
	if err != nil {
		t.Fatalf("Cannot start zookeeper custom rule service: %v", err)
	}
	defer zkcr.Stop()

	var qrs *rules.Rules
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
	conn.Set(ctx, serverPath, []byte(customRule2), -1)
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
	conn.Delete(ctx, serverPath, -1)
	<-time.After(time.Second)
	qrs, _, err = zkcr.GetRules()
	if err != nil {
		t.Fatalf("GetRules of ZkCustomRule should always return nil error, but we receive %v", err)
	}
	if reflect.DeepEqual(qrs, rules.New()) {
		t.Fatalf("Expect empty rule at this point")
	}

	// Test rule path revival
	conn.Create(ctx, serverPath, []byte("customrule2"), 0, zk.WorldACL(zk2topo.PermFile))
	<-time.After(time.Second) //Wait for the polling thread to respond
	qrs, _, err = zkcr.GetRules()
	if err != nil {
		t.Fatalf("GetRules of ZkCustomRule should always return nil error, but we receive %v", err)
	}
	qr = qrs.Find("r2")
	if qr == nil {
		t.Fatalf("Expect custom rule r2 to be found, but got nothing, qrs=%v", qrs)
	}
}
