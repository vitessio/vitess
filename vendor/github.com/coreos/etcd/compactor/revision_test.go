// Copyright 2017 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compactor

import (
	"reflect"
	"testing"
	"time"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/pkg/testutil"
	"github.com/jonboulle/clockwork"
)

func TestRevision(t *testing.T) {
	fc := clockwork.NewFakeClock()
	rg := &fakeRevGetter{testutil.NewRecorderStream(), 0}
	compactable := &fakeCompactable{testutil.NewRecorderStream()}
	tb := &Revision{
		clock:     fc,
		retention: 10,
		rg:        rg,
		c:         compactable,
	}

	tb.Run()
	defer tb.Stop()

	fc.Advance(checkCompactionInterval)
	rg.Wait(1)
	// nothing happens

	rg.rev = 99 // will be 100
	expectedRevision := int64(90)
	fc.Advance(checkCompactionInterval)
	rg.Wait(1)
	a, err := compactable.Wait(1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision}) {
		t.Errorf("compact request = %v, want %v", a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision})
	}

	// skip the same revision
	rg.rev = 99 // will be 100
	expectedRevision = int64(90)
	rg.Wait(1)
	// nothing happens

	rg.rev = 199 // will be 200
	expectedRevision = int64(190)
	fc.Advance(checkCompactionInterval)
	rg.Wait(1)
	a, err = compactable.Wait(1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision}) {
		t.Errorf("compact request = %v, want %v", a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision})
	}
}

func TestRevisionPause(t *testing.T) {
	fc := clockwork.NewFakeClock()
	compactable := &fakeCompactable{testutil.NewRecorderStream()}
	rg := &fakeRevGetter{testutil.NewRecorderStream(), 99} // will be 100
	tb := &Revision{
		clock:     fc,
		retention: 10,
		rg:        rg,
		c:         compactable,
	}

	tb.Run()
	tb.Pause()

	// tb will collect 3 hours of revisions but not compact since paused
	n := int(time.Hour / checkCompactionInterval)
	for i := 0; i < 3*n; i++ {
		fc.Advance(checkCompactionInterval)
	}
	// tb ends up waiting for the clock

	select {
	case a := <-compactable.Chan():
		t.Fatalf("unexpected action %v", a)
	case <-time.After(10 * time.Millisecond):
	}

	// tb resumes to being blocked on the clock
	tb.Resume()

	// unblock clock, will kick off a compaction at hour 3:05
	fc.Advance(checkCompactionInterval)
	rg.Wait(1)
	a, err := compactable.Wait(1)
	if err != nil {
		t.Fatal(err)
	}
	wreq := &pb.CompactionRequest{Revision: int64(90)}
	if !reflect.DeepEqual(a[0].Params[0], wreq) {
		t.Errorf("compact request = %v, want %v", a[0].Params[0], wreq.Revision)
	}
}
