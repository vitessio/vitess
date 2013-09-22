// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/topo"
)

// sandbox_test.go provides a sandbox for unit testing Barnacle.

func init() {
	RegisterDialer("sandbox", sandboxDialer)
}

var (
	// endPointCounter tracks how often GetEndPoints was called
	endPointCounter int

	// endPointMustFail specifies how often GetEndPoints must fail before succeeding
	endPointMustFail int

	// dialerCoun tracks how often sandboxDialer was called
	dialCounter int

	// dialMustFail specifies how often sandboxDialer must fail before succeeding
	dialMustFail int
)

func resetSandbox() {
	testConns = make(map[string]TabletConn)
	endPointCounter = 0
	dialCounter = 0
	dialMustFail = 0
}

type sandboxTopo struct {
}

func (sct *sandboxTopo) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	endPointCounter++
	if endPointMustFail > 0 {
		endPointMustFail--
		return nil, fmt.Errorf("topo error")
	}
	return &topo.EndPoints{Entries: []topo.EndPoint{
		{Host: shard, NamedPortMap: map[string]int{"vt": 1}},
	}}, nil
}

var testConns map[string]TabletConn

func sandboxDialer(addr, keyspace, shard, username, password string, encrypted bool) (TabletConn, error) {
	dialCounter++
	if dialMustFail > 0 {
		dialMustFail--
		return nil, fmt.Errorf("conn error")
	}
	tconn := testConns[addr]
	if tconn == nil {
		panic(fmt.Sprintf("can't find conn %s", addr))
	}
	return tconn, nil
}
