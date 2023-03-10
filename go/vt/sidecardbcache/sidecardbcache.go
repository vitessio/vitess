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

// Package sidecardbcache provides a read through cache of sidecardb
// related data.
package sidecardbcache

import (
	"context"
	"sync"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
)

type cache struct {
	// Lazily loaded cache of sidecar database identifiers by keyspace.
	// The key is a keyspace name string and the val is an sqlparser
	// string built from an IdentifierCS using the sidecar database
	// name stored in the global topo Keyspace record for the given
	// keyspace.
	sidecarDBIdentifiers sync.Map

	// The topo server to use for loading values into the cache.
	toposerv *topo.Server
}

var instance *cache // singleton

// New returns an initialized cache struct. This is a singleton so
// if you call New multiple times you will get the same instance.
func New(topoServer *topo.Server) *cache {
	if instance == nil {
		instance = &cache{
			toposerv:             topoServer,
			sidecarDBIdentifiers: sync.Map{},
		}
	}
	return instance
}

func Get() *cache {
	return instance
}

// GetSidecarDBIdentifierForKeyspace returns an sqlparser string
// built from an IdentifierCS using the sidecar database name
// stored in the global topo Keyspace record for the given
// keyspace. This provides a read through cache of these values
// from the topo server.
func (sc *cache) GetIdentifierForKeyspace(keyspace string) (string, error) {
	if sc.toposerv == nil {
		return "", vterrors.New(vtrpc.Code_FAILED_PRECONDITION, "topology server has not been set")
	}
	sdbid, ok := sc.sidecarDBIdentifiers.Load(keyspace)
	if !ok || sdbid == nil || sdbid == "" {
		ctx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
		defer cancel()

		ki, err := sc.toposerv.GetKeyspace(ctx, keyspace)
		if err != nil {
			return "", err
		}
		if ki.SidecarDbName == "" {
			ki.SidecarDbName = sidecardb.DefaultName
		}

		sdbid = sqlparser.String(sqlparser.NewIdentifierCS(ki.SidecarDbName))
		sc.sidecarDBIdentifiers.Store(keyspace, sdbid)
	}
	return sdbid.(string), nil
}
