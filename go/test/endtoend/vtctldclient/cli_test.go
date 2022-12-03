/*
Copyright 2022 The Vitess Authors.

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

package vtctldclient

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/cmd/vtctldclient/command"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/vtctl/localvtctldclient"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

type fakeServer struct {
	vtctlservicepb.UnimplementedVtctldServer
	t testing.TB

	applySchemaRequests []*vtctldatapb.ApplySchemaRequest
}

func (s *fakeServer) ApplySchema(ctx context.Context, req *vtctldatapb.ApplySchemaRequest) (*vtctldatapb.ApplySchemaResponse, error) {
	s.applySchemaRequests = append(s.applySchemaRequests, req)
	return &vtctldatapb.ApplySchemaResponse{}, nil
}

func TestApplySchema(t *testing.T) {
	server := &fakeServer{t: t}

	command.VtctldClientProtocol = "local"
	localvtctldclient.SetServer(server)

	defer func(argv []string) {
		os.Args = argv
	}(append([]string{}, os.Args...))

	os.Args = []string{
		"vtctldclient",
		"--server='doesnotmatter'",
		"ApplySchema",
		"--sql",
		`"CREATE TABLE foo(id int not null primary key, name varchar(255)); CREATE TABLE bar (id int not null primary key, foo_id int not null);`,
		"test",
	}

	require.NoError(t, command.Root.Execute())
	expected := &vtctldatapb.ApplySchemaRequest{
		Keyspace: "test",
		Sql: []string{
			`"CREATE TABLE foo(id int not null primary key, name varchar(255)); CREATE TABLE bar (id int not null primary key, foo_id int not null);`,
		},
		DdlStrategy:         "direct",
		WaitReplicasTimeout: protoutil.DurationToProto(10 * time.Second),
	}
	actual := server.applySchemaRequests[0]
	assert.True(t, proto.Equal(actual, expected), "ApplySchema received unexpected request (got %v want %v)", actual, expected)
}
