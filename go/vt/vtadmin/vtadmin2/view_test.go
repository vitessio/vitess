/*
Copyright 2026 The Vitess Authors.

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

package vtadmin2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestExternalURLPrefixesHTTPForHosts(t *testing.T) {
	assert.Equal(t, "http://vtgate.example.com:15001", externalURL("vtgate.example.com:15001"))
	assert.Equal(t, "https://vtctld.example.com", externalURL("https://vtctld.example.com"))
	assert.Equal(t, "", externalURL(""))
}

func TestSelectedClusterDefaultsToFirstCluster(t *testing.T) {
	clusters := []*vtadminpb.Cluster{{Id: "local", Name: "Local"}, {Id: "prod", Name: "Prod"}}

	assert.Equal(t, "local", selectedClusterID(clusters, ""))
	assert.Equal(t, "prod", selectedClusterID(clusters, "prod"))
}

func TestSelectedKeyspaceDefaultsToFirstForCluster(t *testing.T) {
	keyspaces := []*vtadminpb.Keyspace{
		{Cluster: &vtadminpb.Cluster{Id: "local"}, Keyspace: &vtctldatapb.Keyspace{Name: "commerce"}},
		{Cluster: &vtadminpb.Cluster{Id: "local"}, Keyspace: &vtctldatapb.Keyspace{Name: "customer"}},
		{Cluster: &vtadminpb.Cluster{Id: "prod"}, Keyspace: &vtctldatapb.Keyspace{Name: "commerce_prod"}},
	}

	assert.Equal(t, "commerce", selectedKeyspaceName(keyspaces, "local", ""))
	assert.Equal(t, "customer", selectedKeyspaceName(keyspaces, "local", "customer"))
	assert.Equal(t, "commerce_prod", selectedKeyspaceName(keyspaces, "prod", ""))
}
