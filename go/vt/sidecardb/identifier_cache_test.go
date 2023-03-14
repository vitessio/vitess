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

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestIdentifierCache(t *testing.T) {
	sidecarDBIdentifierMap := map[string]string{}
	loadFunc := func(ctx context.Context, keyspace string) (string, error) {
		val, ok := sidecarDBIdentifierMap[keyspace]
		if !ok {
			return "", fmt.Errorf("keyspace %s not found", keyspace)
		}
		return val, nil
	}
	// Test using the cache before it's been initialized
	cache, err := GetIdentifierCache()
	require.Error(t, err)
	require.Nil(t, cache)
	require.Equal(t, err.Error(), errIdentifierCacheUninitialized)
	// Create the cache to use for lookups of the sidecar database
	// identifier in use by each keyspace.
	var created bool
	cache, created = NewIdentifierCache(loadFunc)
	require.True(t, created)
	var emptyErr error
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
				newcache, created := NewIdentifierCache(loadFunc) // should work fine
				require.False(t, created)
				if newcache != cache {
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
			wantErr: vterrors.New(vtrpcpb.Code_INTERNAL, errIdentifierCacheNoLoadFunction),
		},
		{
			name:     "delete keyspace",
			keyspace: "ksdel",
			preHook: func() error {
				cache.Delete("ksdel")                   // delete from the cache so we re-load
				delete(sidecarDBIdentifierMap, "ksdel") // delete from the backing database
				return nil
			},
			wantErr: errors.New("keyspace ksdel not found"),
		},
		{
			name:     "clear cache",
			keyspace: "ksalldel",
			preHook: func() error {
				cache.Clear()                                // clear the cache so we re-load
				sidecarDBIdentifierMap = map[string]string{} // clear the backing database
				return nil
			},
			postHook: func() error {
				// Make sure previous entries are also now gone
				_, err := cache.Get("ks1")
				require.Equal(t, err, errors.New("keyspace ks1 not found"))
				_, err = cache.Get("ks3")
				require.Equal(t, err, errors.New("keyspace ks3 not found"))
				return nil
			},
			wantErr: errors.New("keyspace ksalldel not found"),
		},
		{
			name:          "sidecar database name that needs escaping",
			keyspace:      "ks4",
			sidecardbname: "_vt-test",
			want:          "`_vt-test`",
		},
		{
			name:     "destroy cache and create a new one",
			keyspace: "ks5",
			preHook: func() error {
				cache.Destroy()                       // clears the cache and will require a re-load
				delete(sidecarDBIdentifierMap, "ks5") // delete from the backing database
				newcache, created := NewIdentifierCache(loadFunc)
				require.True(t, created)
				if newcache == cache {
					return fmt.Errorf("cache should have been destroyed")
				}
				cache = newcache
				return nil
			},
			wantErr: errors.New("keyspace ks5 not found"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sidecarDBIdentifierMap[tt.keyspace] = tt.sidecardbname
			if tt.preHook != nil {
				err := tt.preHook()
				require.NoError(t, err)
			}
			got, err := cache.Get(tt.keyspace)
			if tt.postHook != nil {
				err := tt.postHook()
				require.NoError(t, err)
			}
			if tt.wantErr != emptyErr && (err == nil || tt.wantErr.Error() != err.Error()) {
				t.Errorf("cache.Get() produced error: %v, wanted error: %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("cache.Get() returned: %v, wanted: %v", got, tt.want)
			}
		})
	}
}
