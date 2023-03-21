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
	"sync"
	"sync/atomic"
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// IdentifierCache provides a read through cache of sidecar database
// identifiers; loading the values from an opaque backend database
// using a provided load function.
type IdentifierCache struct {
	// Lazily loaded cache of sidecar database identifiers by keyspace.
	// The key is a keyspace name string and the val is an sqlparser
	// string built from an IdentifierCS using the sidecar database
	// name stored in the backend database read in the provided load
	// function.
	sidecarDBIdentifiers sync.Map

	// The callback used to load the values from the database into
	// the cache.
	load func(context.Context, string) (string, error)
}

const (
	errIdentifierCacheUninitialized  = "sidecar database identifier cache is not initialized"
	errIdentifierCacheNoLoadFunction = "the load from database function has not been set"
	identifierCacheLoadTimeout       = 30 * time.Second
)

var identifierCache atomic.Value // *IdentifierCache singleton

// NewIdentifierCache returns an initialized cache. This is a
// singleton so if you call New multiple times you will get the
// same instance. If the cache has already been initialized then
// it will return false indicating that your New call did not
// create a new instance.
func NewIdentifierCache(loadFunc func(context.Context, string) (string, error)) (*IdentifierCache, bool) {
	created := identifierCache.CompareAndSwap(nil, &IdentifierCache{
		load:                 loadFunc,
		sidecarDBIdentifiers: sync.Map{},
	})
	return identifierCache.Load().(*IdentifierCache), created
}

func GetIdentifierCache() (*IdentifierCache, error) {
	if identifierCache.Load() == nil {
		return nil, vterrors.New(vtrpcpb.Code_INTERNAL, errIdentifierCacheUninitialized)
	}
	return identifierCache.Load().(*IdentifierCache), nil
}

// Get returns an sqlparser string built from an IdentifierCS using
// the sidecar database name stored in the database. This provides a
// read through cache.
func (ic *IdentifierCache) Get(keyspace string) (string, error) {
	if ic.load == nil {
		return "", vterrors.New(vtrpcpb.Code_INTERNAL, errIdentifierCacheNoLoadFunction)
	}
	sdbid, ok := ic.sidecarDBIdentifiers.Load(keyspace)
	if !ok || sdbid == nil || sdbid == "" {
		ctx, cancel := context.WithTimeout(context.Background(), identifierCacheLoadTimeout)
		defer cancel()

		sdbname, err := ic.load(ctx, keyspace)
		if err != nil {
			return "", err
		}
		if sdbname == "" {
			sdbname = DefaultName
		}

		sdbid = sqlparser.String(sqlparser.NewIdentifierCS(sdbname))
		ic.sidecarDBIdentifiers.Store(keyspace, sdbid)
	}
	return sdbid.(string), nil
}

// GetIdentifierForKeyspace is a convenience function -- combining
// GetIdentifierCache() and IdentifierCache.Get(keyspace) -- which
// returns the sidecar database identifier as an sqlparser string
// for the provided keyspace.
func GetIdentifierForKeyspace(keyspace string) (string, error) {
	cache, err := GetIdentifierCache()
	if err != nil {
		return "", err
	}
	return cache.Get(keyspace)
}

// Delete removes an entry from the cache. It is idempotent and
// will always delete the entry IF it exists.
func (ic *IdentifierCache) Delete(keyspace string) {
	ic.sidecarDBIdentifiers.Delete(keyspace)
}

// Clear empties out the cache.
func (ic *IdentifierCache) Clear() {
	ic.sidecarDBIdentifiers = sync.Map{}
}

// Destroy clears the existing cache and sets the singleton instance
// to nil so that a new cache can be created.
// NOTE: this should ONLY be used in unit tests and NOT in production
// as it breaks the singleton pattern!
func (ic *IdentifierCache) Destroy() {
	ic.Clear()
	identifierCache = atomic.Value{}
}
