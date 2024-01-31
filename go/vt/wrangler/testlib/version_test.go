/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	// Initialize our environment
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1", "cell2")
	wr := wrangler.New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// couple tablets is enough
	sourcePrimary := NewFakeTablet(t, wr, "cell1", 10, topodatapb.TabletType_PRIMARY, nil,
		TabletKeyspaceShard(t, "source", "0"),
		StartHTTPServer())
	sourceReplica := NewFakeTablet(t, wr, "cell1", 11, topodatapb.TabletType_REPLICA, nil,
		TabletKeyspaceShard(t, "source", "0"),
		StartHTTPServer())

	// sourcePrimary loop
	sourcePrimaryGitRev := "fake git rev"
	sourcePrimary.StartActionLoop(t, wr)
	sourcePrimary.HTTPServer.Handler.(*http.ServeMux).HandleFunc("/debug/vars", expvarHandler(&sourcePrimaryGitRev))
	defer sourcePrimary.StopActionLoop(t)

	// sourceReplica loop
	sourceReplicaGitRev := "fake git rev"
	sourceReplica.FakeMysqlDaemon.SetReplicationSourceInputs = append(sourceReplica.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(sourcePrimary.Tablet))
	sourceReplica.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	sourceReplica.StartActionLoop(t, wr)
	sourceReplica.HTTPServer.Handler.(*http.ServeMux).HandleFunc("/debug/vars", expvarHandler(&sourceReplicaGitRev))
	defer sourceReplica.StopActionLoop(t)

	// test when versions are the same
	sourceReplicaGitRev = "fake git rev"
	if err := vp.Run([]string{"ValidateVersionKeyspace", sourcePrimary.Tablet.Keyspace}); err != nil {
		t.Fatalf("ValidateVersionKeyspace(same) failed: %v", err)
	}

	// test when versions are different
	sourceReplicaGitRev = "different fake git rev"
	err := vp.Run([]string{"ValidateVersionKeyspace", sourcePrimary.Tablet.Keyspace})
	fmt.Printf("ERROR %v", err)
	if err == nil || !strings.Contains(err.Error(), "is different than replica") {
		t.Fatalf("ValidateVersionKeyspace(different) returned an unexpected error: %v", err)
	}
}
