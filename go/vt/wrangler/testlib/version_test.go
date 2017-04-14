// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func expvarHandler(gitRev *string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		var vars struct {
			BuildHost      string
			BuildUser      string
			BuildTimestamp int64
			BuildGitRev    string
		}
		vars.BuildHost = "fake host"
		vars.BuildUser = "fake user"
		vars.BuildTimestamp = 123
		vars.BuildGitRev = *gitRev
		result, err := json.Marshal(&vars)
		if err != nil {
			http.Error(w, fmt.Sprintf("cannot marshal json: %s", err), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, string(result)+"\n")
	}
}

func TestVersion(t *testing.T) {
	// We need to run this test with the /debug/vars version of the
	// plugin.
	wrangler.ResetDebugVarsGetVersion()

	// Initialize our environment
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// couple tablets is enough
	sourceMaster := NewFakeTablet(t, wr, "cell1", 10, topodatapb.TabletType_MASTER, nil,
		TabletKeyspaceShard(t, "source", "0"),
		StartHTTPServer())
	sourceReplica := NewFakeTablet(t, wr, "cell1", 11, topodatapb.TabletType_REPLICA, nil,
		TabletKeyspaceShard(t, "source", "0"),
		StartHTTPServer())

	// sourceMaster loop
	sourceMasterGitRev := "fake git rev"
	sourceMaster.StartActionLoop(t, wr)
	sourceMaster.HTTPServer.Handler.(*http.ServeMux).HandleFunc("/debug/vars", expvarHandler(&sourceMasterGitRev))
	defer sourceMaster.StopActionLoop(t)

	// sourceReplica loop
	sourceReplicaGitRev := "fake git rev"
	sourceReplica.StartActionLoop(t, wr)
	sourceReplica.HTTPServer.Handler.(*http.ServeMux).HandleFunc("/debug/vars", expvarHandler(&sourceReplicaGitRev))
	defer sourceReplica.StopActionLoop(t)

	// test when versions are the same
	if err := vp.Run([]string{"ValidateVersionKeyspace", sourceMaster.Tablet.Keyspace}); err != nil {
		t.Fatalf("ValidateVersionKeyspace(same) failed: %v", err)
	}

	// test when versions are different
	sourceReplicaGitRev = "different fake git rev"
	if err := vp.Run([]string{"ValidateVersionKeyspace", sourceMaster.Tablet.Keyspace}); err == nil || !strings.Contains(err.Error(), "is different than slave") {
		t.Fatalf("ValidateVersionKeyspace(different) returned an unexpected error: %v", err)
	}
}
