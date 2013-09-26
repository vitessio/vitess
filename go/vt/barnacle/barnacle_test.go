// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/barnacle/proto"
)

// This file uses the sandbox_test framework.

func TestGetSessionId(t *testing.T) {
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	Init(blm, "sandbox", 1*time.Second, 10)

	var sess proto.Session
	err := RpcBarnacle.GetSessionId(&proto.SessionParams{TabletType: "master"}, &sess)
	if err != nil {
		t.Errorf("%v", err)
	}
	if sess.SessionId == 0 {
		t.Errorf("want non-zero, got 0")
	}

	var next proto.Session
	err = RpcBarnacle.GetSessionId(&proto.SessionParams{TabletType: "master"}, &next)
	if err != nil {
		t.Errorf("%v", err)
	}
	if sess.SessionId == next.SessionId {
		t.Errorf("want unequal, got equal %d", sess.SessionId)
	}
}
