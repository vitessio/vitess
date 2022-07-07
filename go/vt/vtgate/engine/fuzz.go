//go:build gofuzz
// +build gofuzz

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

/*
	DEPENDENCIES:
	This fuzzer relies heavily on
	$VTROOT/go/vt/vtgate/engine/fake_vcursor_test.go,
	and in order to run it, it is required to rename:
	$VTROOT/go/vt/vtgate/engine/fake_vcursor_test.go
	to
	$VTROOT/go/vt/vtgate/engine/fake_vcursor.go

	This is handled by the OSS-fuzz build script and
	is only important to make note of if the fuzzer
	is run locally.

	STATUS:
	The fuzzer does not currently implement executions
	for all possible API's in the engine package, and
	it can be considered experimental, as I (@AdamKorcz)
	am interested in its performance when being run
	continuously by OSS-fuzz. Needless to say, more
	APIs can be added with ease.
*/

package engine

import (
	fuzz "github.com/AdaLogics/go-fuzz-headers"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// FuzzEngine implements the fuzzer
func FuzzEngine(data []byte) int {
	c := fuzz.NewConsumer(data)

	index, err := c.GetInt()
	if err != nil {
		return 0
	}
	switch i := index % 3; i {
	case 0:
		execUpdate(c)
	case 1:
		execInsert(c)
	case 2:
		execRoute(c)
	default:
		return 0
	}
	return 1
}

// execUpdate implements a wrapper to fuzz Update.Tryexecute()
func execUpdate(f *fuzz.ConsumeFuzzer) {
	upd := &Update{}
	err := f.GenerateStruct(upd)
	if err != nil {
		return
	}
	vc := &loggingVCursor{}
	_, _ = upd.TryExecute(ctx, vc, map[string]*querypb.BindVariable{}, false)
}

// execUpdate implements a wrapper to fuzz Insert.Tryexecute()
func execInsert(f *fuzz.ConsumeFuzzer) {
	ins := &Insert{}
	err := f.GenerateStruct(ins)
	if err != nil {
		return
	}
	vc := &loggingVCursor{}
	_, _ = ins.TryExecute(ctx, vc, map[string]*querypb.BindVariable{}, false)
}

// execUpdate implements a wrapper to fuzz Route.Tryexecute()
func execRoute(f *fuzz.ConsumeFuzzer) {
	sel := &Route{}
	err := f.GenerateStruct(sel)
	if err != nil {
		return
	}
	vc := newFuzzDMLTestVCursor("0")
	_, _ = sel.TryExecute(ctx, vc, map[string]*querypb.BindVariable{}, false)
}

func newFuzzDMLTestVCursor(shards ...string) *loggingVCursor {
	return &loggingVCursor{shards: shards, resolvedTargetTabletType: topodatapb.TabletType_PRIMARY}
}
