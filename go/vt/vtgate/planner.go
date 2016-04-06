// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

// This is a V3 file. Do not intermix with V2.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/cache"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// Planner is used to compute the plan. It contains
// the vschema, and has a cache of previous computed plans.
type Planner struct {
	serv    topo.SrvTopoServer
	cell    string
	mu      sync.Mutex
	vschema *vindexes.VSchema
	plans   *cache.LRUCache
}

var once sync.Once

// NewPlanner creates a new planner for VTGate.
// It will watch the vschema in the topology until the ctx is closed.
func NewPlanner(ctx context.Context, serv topo.SrvTopoServer, cell string, cacheSize int) *Planner {
	plr := &Planner{
		serv:  serv,
		cell:  cell,
		plans: cache.NewLRUCache(int64(cacheSize)),
	}
	plr.WatchVSchema(ctx)
	once.Do(func() {
		http.Handle("/debug/query_plans", plr)
		http.Handle("/debug/vschema", plr)
	})
	return plr
}

// WatchVSchema watches the VSchema from the topo. The function does
// not return an error. It instead logs warnings on failure.
// We get the list of keyspaces to watch at the beginning (because it's easier)
// and we don't watch that. So when adding a new keyspace, for now,
// vtgate needs to be restarted (after having rebuilt the serving graph).
// This could be fixed by adding a WatchSrvKeyspaceNames API to topo.Server,
// and when it triggers we'd diff the values, and terminate the missing
// SrvKeyspaces, and start watching the new SrvKeyspaces.
func (plr *Planner) WatchVSchema(ctx context.Context) {
	keyspaces, err := plr.serv.GetSrvKeyspaceNames(ctx, plr.cell)
	if err != nil {
		log.Warningf("Error loading vschema: could not read keyspaces: %v", err)
		return
	}

	// we will wait until we get the first value for each keyspace
	// before returning
	var wg sync.WaitGroup

	// mu protects formal
	var mu sync.Mutex
	formal := &vindexes.VSchemaFormal{
		Keyspaces: make(map[string]vindexes.KeyspaceFormal),
	}
	processKeyspace := func(keyspace, kschema string) {
		// Note we use a closure here to allow for defer()
		// to work properly, and also to let the caller
		// do more things even if this fails.

		// unpack the new VSchema, or skip it
		var kformal vindexes.KeyspaceFormal
		if err := json.Unmarshal([]byte(kschema), &kformal); err != nil {
			log.Warningf("Error unmarshalling vschema for keyspace %s: %v", keyspace, err)
			return
		}

		// rebuild the new component
		mu.Lock()
		defer mu.Unlock()
		formal.Keyspaces[keyspace] = kformal
		vschema, err := vindexes.BuildVSchema(formal)
		if err != nil {
			log.Warningf("Error creating VSchema: %v", err)
			return
		}

		plr.mu.Lock()
		plr.vschema = vschema
		plr.mu.Unlock()
	}

	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			gotFirstValue := false

			notifications, err := plr.serv.WatchVSchema(ctx, keyspace)
			if err != nil {
				log.Warningf("Error watching vschema for keyspace %s, will not watch it: %v", keyspace, err)
				return
			}

			for kschema := range notifications {
				processKeyspace(keyspace, kschema)

				if !gotFirstValue {
					gotFirstValue = true
					wg.Done()
				}
			}
		}(keyspace)
	}

	wg.Wait()
}

// VSchema returns the VSchema.
func (plr *Planner) VSchema() *vindexes.VSchema {
	plr.mu.Lock()
	defer plr.mu.Unlock()
	return plr.vschema
}

// GetPlan computes the plan for the given query. If one is in
// the cache, it reuses it.
func (plr *Planner) GetPlan(sql, keyspace string) (*engine.Plan, error) {
	if plr.VSchema() == nil {
		return nil, errors.New("vschema not initialized")
	}
	key := sql
	if keyspace != "" {
		key = keyspace + ":" + sql
	}
	if result, ok := plr.plans.Get(key); ok {
		return result.(*engine.Plan), nil
	}
	plan, err := planbuilder.Build(sql, &wrappedVSchema{
		vschema:  plr.VSchema(),
		keyspace: keyspace,
	})
	if err != nil {
		return nil, err
	}
	plr.plans.Set(sql, plan)
	return plan, nil
}

// ServeHTTP shows the current plans in the query cache.
func (plr *Planner) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	if request.URL.Path == "/debug/query_plans" {
		keys := plr.plans.Keys()
		response.Header().Set("Content-Type", "text/plain")
		response.Write([]byte(fmt.Sprintf("Length: %d\n", len(keys))))
		for _, v := range keys {
			response.Write([]byte(fmt.Sprintf("%#v\n", v)))
			if plan, ok := plr.plans.Peek(v); ok {
				if b, err := json.MarshalIndent(plan, "", "  "); err != nil {
					response.Write([]byte(err.Error()))
				} else {
					response.Write(b)
				}
				response.Write(([]byte)("\n\n"))
			}
		}
	} else if request.URL.Path == "/debug/vschema" {
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		b, err := json.MarshalIndent(plr.VSchema(), "", " ")
		if err != nil {
			response.Write([]byte(err.Error()))
			return
		}
		buf := bytes.NewBuffer(nil)
		json.HTMLEscape(buf, b)
		response.Write(buf.Bytes())
	} else {
		response.WriteHeader(http.StatusNotFound)
	}
}

type wrappedVSchema struct {
	vschema  *vindexes.VSchema
	keyspace string
}

func (vs *wrappedVSchema) Find(keyspace, tablename string) (table *vindexes.Table, err error) {
	if keyspace == "" {
		keyspace = vs.keyspace
	}
	return vs.vschema.Find(keyspace, tablename)
}
