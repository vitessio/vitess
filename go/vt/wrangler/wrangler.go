// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkwrangler

import (
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/zk"
)

type Wrangler struct {
	zconn zk.Conn
	ai    *tm.ActionInitiator
}

func NewWrangler(zconn zk.Conn, ai *tm.ActionInitiator) *Wrangler {
	return &Wrangler{zconn, ai}
}

func (wr *Wrangler) readTablet(zkTabletPath string) (*tm.TabletInfo, error) {
	return tm.ReadTablet(wr.zconn, zkTabletPath)
}
