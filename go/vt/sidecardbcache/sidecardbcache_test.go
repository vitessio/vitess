/*
Copyright 2023 The Vitess Authors.

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

package sidecardbcache

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestAll(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	cache := New(ts)
	tests := []struct {
		name          string
		keyspace      string
		sidecardbname string
		preHook       func() error
		postHook      func() error
		want          string
		wantErr       bool
	}{
		{
			name: "calling New twice should return the same instance",
			preHook: func() error {
				cache = New(ts) // should work fine
				if cache != New(ts) {
					return fmt.Errorf("cache should be singleton")
				}
				return nil
			},
			keyspace:      "ks1",
			sidecardbname: "_vt",
			want:          "_vt",
			wantErr:       false,
		},
		{
			name:     "keyspace doesn't exist",
			keyspace: "ks2",
			preHook: func() error {
				return ts.DeleteKeyspace(context.Background(), "ks2")
			},
			want:    "",
			wantErr: true,
		},
		{
			name:     "uninitialized cache",
			keyspace: "ks3",
			preHook: func() error {
				cache.toposerv = nil
				return nil
			},
			postHook: func() error {
				cache.toposerv = ts
				return nil
			},
			want:    "",
			wantErr: true,
		},
		{
			name:          "sidecar database name that needs escaping",
			keyspace:      "ks4",
			sidecardbname: "_vt-test",
			want:          "`_vt-test`",
			wantErr:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ts.CreateKeyspace(context.Background(), tt.keyspace, &topodatapb.Keyspace{
				SidecarDbName: tt.sidecardbname,
			})
			require.NoError(t, err)
			if tt.preHook != nil {
				err := tt.preHook()
				require.NoError(t, err)
			}
			got, err := cache.GetIdentifierForKeyspace(tt.keyspace)
			if tt.postHook != nil {
				err := tt.postHook()
				require.NoError(t, err)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("cache.GetIdentifierForKeyspace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("cache.GetIdentifierForKeyspace() = %v, want %v", got, tt.want)
			}
		})
	}
}
