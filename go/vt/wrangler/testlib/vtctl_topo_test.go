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

package testlib

import (
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo/memorytopo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func testVtctlTopoCommand(t *testing.T, vp *VtctlPipe, args []string, want string) {
	got, err := vp.RunAndOutput(args)
	if err != nil {
		t.Fatalf("testVtctlTopoCommand(%v) failed: %v", args, err)
	}

	// Remove the variable version numbers.
	lines := strings.Split(got, "\n")
	for i, line := range lines {
		if vi := strings.Index(line, "version="); vi != -1 {
			lines[i] = line[:vi+8] + "V"
		}
	}
	got = strings.Join(lines, "\n")
	if got != want {
		t.Errorf("testVtctlTopoCommand(%v) failed: got:\n%vwant:\n%v", args, got, want)
	}
}

// TestVtctlTopoCommands tests all vtctl commands from the
// "Topo" group.
func TestVtctlTopoCommands(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	if err := ts.CreateKeyspace(context.Background(), "ks1", &topodatapb.Keyspace{ShardingColumnName: "col1"}); err != nil {
		t.Fatalf("CreateKeyspace() failed: %v", err)
	}
	if err := ts.CreateKeyspace(context.Background(), "ks2", &topodatapb.Keyspace{ShardingColumnName: "col2"}); err != nil {
		t.Fatalf("CreateKeyspace() failed: %v", err)
	}
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Test TopoCat.
	testVtctlTopoCommand(t, vp, []string{"TopoCat", "-long", "-decode_proto", "/keyspaces/*/Keyspace"}, `path=/keyspaces/ks1/Keyspace version=V
sharding_column_name: "col1"
path=/keyspaces/ks2/Keyspace version=V
sharding_column_name: "col2"
`)
}
