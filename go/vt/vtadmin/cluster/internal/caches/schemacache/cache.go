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

// Package schemacache provides wrapper functions for interacting with
// instances of the generic (vtadmin/cache).Cache that store schemas.
package schemacache

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/vtadmin/cache"
	"vitess.io/vitess/go/vt/vterrors"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

// Key is the cache key for vtadmin's schema caches.
type Key struct {
	ClusterID               string
	Keyspace                string
	IncludeNonServingShards bool
}

// Key implements cache.Keyer for Key.
func (k Key) Key() string {
	return fmt.Sprintf("%s/%s/%v", k.ClusterID, k.Keyspace, k.IncludeNonServingShards)
}

type schemaCache = cache.Cache[Key, []*vtadminpb.Schema] // define a type alias for brevity within this package.

// AddOrBackfill takes a (schemas, key, duration) triple (see cache.Add) and a
// LoadOptions and either adds the schemas to the cache directly for the key, or
// enqueues a backfill for the key. Which of these occurs depends on the given
// LoadOptions. If the options, when passed to Load(All|One) would result in a
// full payload of the cached data, then the schemas are added to the cache
// directly. Otherwise, data would be missing for future Load(All|One) calls,
// so we enqueue a backfill to fetch the full payload and add that to the cache.
//
// Callers are expected to call this function in the background, so it returns
// nothing and only logs warnings on failures.
func AddOrBackfill(c *schemaCache, schemas []*vtadminpb.Schema, key Key, d time.Duration, opts LoadOptions) {
	if opts.isFullPayload() {
		if err := c.Add(key, schemas, d); err != nil {
			log.Warningf("failed to add schema to cache for %+v: %s", key, err)
		}
	} else {
		if !c.EnqueueBackfill(key) {
			log.Warningf("failed to enqueue backfill for schema cache %+v", key)
		}
	}
}

// LoadOptions is the set of options used by Load(All|One) to filter down fully-
// cached schema payloads.
type LoadOptions struct {
	BaseRequest    *vtctldatapb.GetSchemaRequest
	AggregateSizes bool
	filter         *tmutils.TableFilter
}

// isFullPayload returns true if the schema(s) returned by calls to
// Load(All|One) from these options represents the full cached payload. if
// false, Load(All|One) will return only a subset of what is in the cache,
// meaning that AddOrBackfill for these options should enqueue a backfill to
// fetch the full payload.
func (opts LoadOptions) isFullPayload() bool {
	if opts.BaseRequest == nil {
		return false
	}

	if len(opts.BaseRequest.Tables) > 0 {
		return false
	}

	if len(opts.BaseRequest.ExcludeTables) > 0 {
		return false
	}

	if !opts.BaseRequest.IncludeViews {
		return false
	}

	if opts.BaseRequest.TableNamesOnly {
		return false
	}

	if opts.BaseRequest.TableSizesOnly {
		return false
	}

	if opts.BaseRequest.TableSchemaOnly {
		return false
	}

	if !opts.AggregateSizes {
		return false
	}

	return true
}

// LoadAll returns the slice of all schemas cached for the given key, filtering
// down based on LoadOptions.
//
// The boolean return value will be false if there is nothing in the cache for
// key.
func LoadAll(c *schemaCache, key Key, opts LoadOptions) (schemas []*vtadminpb.Schema, ok bool, err error) {
	cachedSchemas, ok := c.Get(key)
	if !ok {
		return nil, false, nil
	}

	opts.filter, err = tmutils.NewTableFilter(opts.BaseRequest.Tables, opts.BaseRequest.ExcludeTables, opts.BaseRequest.IncludeViews)
	if err != nil {
		// Note that there's no point in falling back to a full fetch in this
		// case; this exact code gets executed on the tabletserver side, too,
		// so we would just fail on the same issue _after_ paying the cost to
		// wait for a roundtrip to the remote tablet.
		return nil, ok, err
	}

	schemas = make([]*vtadminpb.Schema, 0, len(cachedSchemas))
	for _, schema := range cachedSchemas {
		if key.Keyspace != "" && schema.Keyspace != key.Keyspace {
			continue
		}

		schemas = append(schemas, loadSchema(schema, opts))
	}

	return schemas, ok, nil
}

// LoadOne loads a single schema for the given key, filtering down based on the
// LoadOptions. If there is not exactly one schema for the key, an error is
// returned.
//
// The boolean return value will be false if there is nothing in the cache for
// key.
func LoadOne(c *schemaCache, key Key, opts LoadOptions) (schema *vtadminpb.Schema, found bool, err error) {
	schemas, found, err := LoadAll(c, key, opts)
	if err != nil {
		return nil, found, err
	}

	if !found {
		return nil, found, nil
	}

	if len(schemas) != 1 {
		err = vterrors.Errorf(vtrpc.Code_INTERNAL, "impossible: cache should have exactly 1 schema for request %+v (have %d)", key, len(schemas))
		log.Errorf(err.Error())
		return nil, found, err
	}

	return schemas[0], found, nil
}

func loadSchema(cachedSchema *vtadminpb.Schema, opts LoadOptions) *vtadminpb.Schema {
	schema := proto.Clone(cachedSchema).(*vtadminpb.Schema)

	if !opts.AggregateSizes {
		schema.TableSizes = nil
	}

	tables := make([]*tabletmanagerdatapb.TableDefinition, 0, len(schema.TableDefinitions))
	tableSizes := make(map[string]*vtadminpb.Schema_TableSize, len(schema.TableSizes))

	for _, td := range schema.TableDefinitions {
		if !opts.filter.Includes(td.Name, td.Type) {
			continue
		}

		if opts.AggregateSizes {
			tableSizes[td.Name] = schema.TableSizes[td.Name]
		}

		if opts.BaseRequest.TableNamesOnly {
			tables = append(tables, &tabletmanagerdatapb.TableDefinition{
				Name: td.Name,
			})
			continue
		}

		if opts.BaseRequest.TableSizesOnly {
			tables = append(tables, &tabletmanagerdatapb.TableDefinition{
				Name:       td.Name,
				DataLength: td.DataLength,
				RowCount:   td.RowCount,
			})
			continue
		}

		tables = append(tables, td)
	}

	schema.TableDefinitions = tables
	schema.TableSizes = tableSizes

	return schema
}
