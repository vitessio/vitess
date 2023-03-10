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

package sidecardb

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestAll(t *testing.T) {
	sidecarDBIdentifierMap := map[string]string{}
	loadFunc := func(ctx context.Context, keyspace string) (string, error) {
		val, ok := sidecarDBIdentifierMap[keyspace]
		if !ok {
			return "", fmt.Errorf("keyspace %s not found", keyspace)
		}
		return val, nil
	}
	// Create a cache to use for lookups of the sidecar database identifier
	// in use by each keyspace.
	cache := NewIdentifierCache(loadFunc)
	tests := []struct {
		name          string
		keyspace      string
		sidecardbname string
		preHook       func() error
		postHook      func() error
		want          string
		wantErr       error
	}{
		{
			name: "calling New twice should return the same instance",
			preHook: func() error {
				cache = NewIdentifierCache(loadFunc) // should work fine
				if cache != NewIdentifierCache(loadFunc) {
					return fmt.Errorf("cache should be singleton")
				}
				return nil
			},
			keyspace:      "ks1",
			sidecardbname: "_vt",
			want:          "_vt",
		},
		{
			name:     "keyspace doesn't exist",
			keyspace: "ks2",
			preHook: func() error {
				delete(sidecarDBIdentifierMap, "ks2")
				return nil
			},
			wantErr: errors.New("keyspace ks2 not found"),
		},
		{
			name:     "uninitialized load func",
			keyspace: "ks3",
			preHook: func() error {
				cache.load = nil
				return nil
			},
			postHook: func() error {
				cache.load = loadFunc
				return nil
			},
			wantErr: vterrors.New(vtrpc.Code_INTERNAL, ErrIdentifierCacheNoLoadFunction),
		},
		{
			name:          "sidecar database name that needs escaping",
			keyspace:      "ks4",
			sidecardbname: "_vt-test",
			want:          "`_vt-test`",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sidecarDBIdentifierMap[tt.keyspace] = tt.sidecardbname
			if tt.preHook != nil {
				err := tt.preHook()
				require.NoError(t, err)
			}
			got, err := cache.GetForKeyspace(tt.keyspace)
			if tt.postHook != nil {
				err := tt.postHook()
				require.NoError(t, err)
			}
			if err != nil && err.Error() != tt.wantErr.Error() {
				t.Errorf("cache.GetIdentifierForKeyspace() error = %v, wantErr: %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("cache.GetIdentifierForKeyspace() = %v, want %v", got, tt.want)
			}
		})
	}
}
