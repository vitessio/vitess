/*
Copyright 2021 The Vitess Authors.

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

package schema

import (
	"testing"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"
)

func TestTracking(t *testing.T) {
	target := &querypb.Target{
		Keyspace:   "ks",
		Shard:      "-80",
		TabletType: topodatapb.TabletType_MASTER,
		Cell:       "aa",
	}
	tablet := &topodatapb.Tablet{}
	sbc := sandboxconn.NewSandboxConn(tablet)
	ch := make(chan *discovery.TabletHealth)
	defer func() {
		close(ch)
	}()
	tracker := NewTracker(ch)
	tracker.Start()
	defer tracker.Stop()

	ch <- &discovery.TabletHealth{
		Conn:    sbc,
		Tablet:  tablet,
		Target:  target,
		Serving: true,
	}
	cols := tracker.GetColumns("ks", "t")
	var expectedCols vindexes.Column
	utils.MustMatch(t, expectedCols, cols, "")
}
