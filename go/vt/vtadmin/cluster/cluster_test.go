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

package cluster_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery/fakediscovery"
	"vitess.io/vitess/go/vt/vtadmin/vtsql"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

func TestGetTablets(t *testing.T) {
	disco := fakediscovery.New()
	disco.AddTaggedGates(nil, &vtadminpb.VTGate{Hostname: "gate"})

	db := vtsql.New(&vtsql.Config{
		Cluster: &vtadminpb.Cluster{
			Id:   "c1",
			Name: "one",
		},
		Discovery: disco,
	})
	db.DialFunc = func(cfg vitessdriver.Configuration) (*sql.DB, error) {
		return nil, assert.AnError
	}

	c := &cluster.Cluster{DB: db}
	_, err := c.GetTablets(context.Background())
	assert.Error(t, err)
}
