// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"fmt"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
)

var vtmap = map[string]int{
	"vt": 1,
}

type SimpleTopoServ struct {
}

func (tps *SimpleTopoServ) GetSrvTabletType(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.VtnsAddrs, error) {
	return &topo.VtnsAddrs{Entries: []topo.VtnsAddr{
		{Host: "0", NamedPortMap: vtmap},
		{Host: "1", NamedPortMap: vtmap},
		{Host: "2", NamedPortMap: vtmap},
	}}, nil
}

func TestSimple(t *testing.T) {
	tps := NewBCTopo(new(SimpleTopoServ), "aa", "vt", 1*time.Second)
	blc := tps.Balancer("test_keyspace", "0", "master")
	blc2 := tps.Balancer("test_keyspace", "0", "master")
	if blc != blc2 {
		t.Errorf("Balancers don't match, map is %v", tps.balancers)
	}
	blc3 := tps.Balancer("other_keyspace", "0", "master")
	if blc == blc3 {
		t.Errorf("Balancers match, map is %v", tps.balancers)
	}
	for i := 0; i < 3; i++ {
		addr, _ := blc.Get()
		if addr == "0:1" {
			return
		}
	}
	t.Errorf("address 0:1 not found")
}

func TestPortError(t *testing.T) {
	tps := NewBCTopo(new(SimpleTopoServ), "aa", "noport", 1*time.Second)
	blc := tps.Balancer("test_keyspace", "0", "master")
	got, err := blc.Get()
	if got != "" {
		t.Errorf("want empty, got %s", got)
	}
	if err.Error() != "named port noport not found in map[vt:1]" {
		t.Errorf("want named port noport not found in map[vt:1], got %v", err)
	}
}

type ErrorTopoServ struct {
}

func (tps *ErrorTopoServ) GetSrvTabletType(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.VtnsAddrs, error) {
	return nil, fmt.Errorf("topo error")
}

func TestTopoError(t *testing.T) {
	tps := NewBCTopo(new(ErrorTopoServ), "aa", "vt", 1*time.Second)
	blc := tps.Balancer("test_keyspace", "0", "master")
	got, err := blc.Get()
	if got != "" {
		t.Errorf("want empty, got %s", got)
	}
	if err.Error() != "topo error" {
		t.Errorf("want topo error, got %v", err)
	}
}
