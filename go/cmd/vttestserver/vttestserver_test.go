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

package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/vt/vttest"

	"github.com/golang/protobuf/jsonpb"
	"vitess.io/vitess/go/vt/proto/logutil"
	"vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"
)

type columnVindex struct {
	keyspace   string
	table      string
	vindex     string
	vindexType string
	column     string
}

func TestRunsVschemaMigrations(t *testing.T) {
	schemaDirArg := "-schema_dir=data/schema"
	webDirArg := "-web_dir=web/vtctld/app"
	webDir2Arg := "-web_dir2=web/vtctld2/app"
	tabletHostname := "-tablet_hostname=localhost"
	keyspaceArg := "-keyspaces=test_keyspace,app_customer"
	numShardsArg := "-num_shards=2,2"

	os.Args = append(os.Args, []string{schemaDirArg, keyspaceArg, numShardsArg, webDirArg, webDir2Arg, tabletHostname}...)

	cluster := runCluster()
	defer cluster.TearDown()

	assertColumnVindex(t, cluster, columnVindex{keyspace: "test_keyspace", table: "test_table", vindex: "my_vdx", vindexType: "hash", column: "id"})
	assertColumnVindex(t, cluster, columnVindex{keyspace: "app_customer", table: "customers", vindex: "hash", vindexType: "hash", column: "id"})
}

func assertColumnVindex(t *testing.T, cluster vttest.LocalCluster, expected columnVindex) {
	server := fmt.Sprintf("localhost:%v", cluster.GrpcPort())
	args := []string{"GetVSchema", expected.keyspace}
	ctx := context.Background()

	err := vtctlclient.RunCommandAndWait(ctx, server, args, func(e *logutil.Event) {
		var keyspace vschema.Keyspace
		if err := jsonpb.UnmarshalString(e.Value, &keyspace); err != nil {
			t.Error(err)
		}

		columnVindex := keyspace.Tables[expected.table].ColumnVindexes[0]
		actualVindex := keyspace.Vindexes[expected.vindex]
		assertEqual(t, actualVindex.Type, expected.vindexType, "Actual vindex type different from expected")
		assertEqual(t, columnVindex.Name, expected.vindex, "Actual vindex name different from expected")
		assertEqual(t, columnVindex.Columns[0], expected.column, "Actual vindex column different from expected")
	})
	if err != nil {
		t.Error(err)
	}
}

func assertEqual(t *testing.T, actual string, expected string, message string) {
	if actual != expected {
		t.Errorf("%s: actual %s, expected %s", message, actual, expected)
	}
}
