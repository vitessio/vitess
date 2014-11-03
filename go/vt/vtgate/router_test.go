// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"path"
	"testing"
	"time"

	"github.com/youtube/vitess/go/testfiles"
	"github.com/youtube/vitess/go/vt/context"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

type VTGateSchemaNormalized struct {
	Keyspaces map[string]struct {
		ShardingScheme int
		Indexes        map[string]struct {
			// Type is ShardKey or Lookup.
			Type      int
			From, To  string
			Owner     string
			IsAutoInc bool
		}
		Tables map[string]struct {
			IndexColumns []struct {
				Column    string
				IndexName string
			}
		}
	}
}

func TestSelectSingleShardKey(t *testing.T) {
	schema, err := planbuilder.LoadSchemaJSON(locateFile("router_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	s := createSandbox("TestRouter")
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("-20", sbc1)
	s.MapTestConn("40-60", sbc2)
	serv := new(sandboxTopo)
	scatterConn := NewScatterConn(serv, "", "aa", 1*time.Second, 10, 1*time.Millisecond)
	router := NewRouter(serv, "aa", schema, "", scatterConn)
	q := proto.Query{
		Sql:        "select * from user where id = 1",
		TabletType: topo.TYPE_MASTER,
	}
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("want 1, got %v\n", sbc1.ExecCount)
	}
	if sbc2.ExecCount != 0 {
		t.Errorf("want 0, got %v\n", sbc2.ExecCount)
	}
	q.Sql = "select * from user where id = 3"
	_, err = router.Execute(&context.DummyContext{}, &q)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("want 1, got %v\n", sbc1.ExecCount)
	}
	if sbc2.ExecCount != 1 {
		t.Errorf("want 1, got %v\n", sbc2.ExecCount)
	}
}

func locateFile(name string) string {
	if path.IsAbs(name) {
		return name
	}
	return testfiles.Locate("vtgate/" + name)
}
