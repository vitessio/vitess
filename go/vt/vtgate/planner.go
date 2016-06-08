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
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/cache"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"

	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
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

var plannerOnce sync.Once

// NewPlanner creates a new planner for VTGate.
// It will watch the vschema in the topology until the ctx is closed.
func NewPlanner(ctx context.Context, serv topo.SrvTopoServer, cell string, cacheSize int) *Planner {
	plr := &Planner{
		serv:  serv,
		cell:  cell,
		plans: cache.NewLRUCache(int64(cacheSize)),
	}
	plr.WatchSrvVSchema(ctx, cell)
	plannerOnce.Do(func() {
		http.Handle("/debug/query_plans", plr)
		http.Handle("/debug/vschema", plr)
	})
	return plr
}

// WatchSrvVSchema watches the SrvVSchema from the topo. The function does
// not return an error. It instead logs warnings on failure.
// The SrvVSchema object is roll-up of all the Keyspace information,
// so when a keyspace is added or removed, it will be properly updated.
//
// This function will wait until the first value has either been processed
// or triggered an error before returning.
func (plr *Planner) WatchSrvVSchema(ctx context.Context, cell string) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		foundFirstValue := false

		// Create a closure to save the vschema. If the value
		// passed is nil, it means we encountered an error and
		// we don't know the real value. In this case, we want
		// to use the previous value if it was set, or an
		// empty vschema if it wasn't.
		saveVSchema := func(v *vschemapb.SrvVSchema) {
			// transform the provided SrvVSchema into a VSchema
			var vschema *vindexes.VSchema
			if v != nil {
				var err error
				vschema, err = vindexes.BuildVSchema(v)
				if err != nil {
					log.Warningf("Error creating VSchema for cell %v (will try again next update): %v", cell, err)
					v = nil
				}
			}
			if v == nil {
				// we encountered an error, build an
				// empty vschema
				vschema, _ = vindexes.BuildVSchema(&vschemapb.SrvVSchema{})
			}

			// save our value
			plr.mu.Lock()
			if v != nil {
				// no errors, we can save our schema
				plr.vschema = vschema
			} else {
				// we had an error, use the empty vschema
				// if we had nothing before.
				if plr.vschema == nil {
					plr.vschema = vschema
				}
			}
			plr.mu.Unlock()
			plr.plans.Clear()

			// notify the listener
			if !foundFirstValue {
				foundFirstValue = true
				wg.Done()
			}
		}

		for {
			n, err := plr.serv.WatchSrvVSchema(ctx, cell)
			if err != nil {
				log.Warningf("Error watching vschema for cell %s (will wait 5s before retrying): %v", cell, err)
				saveVSchema(nil)
				time.Sleep(5 * time.Second)
				continue
			}

			for value := range n {
				if value == nil {
					log.Warningf("Got an empty vschema for cell %v", cell)
					saveVSchema(&vschemapb.SrvVSchema{})
					continue
				}

				saveVSchema(value)
			}

			log.Warningf("Watch on vschema for cell %v ended, will wait 5s before retrying", cell)
			saveVSchema(nil)
			time.Sleep(5 * time.Second)
		}
	}()

	// wait for the first value to have been processed
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
		b, err := json.MarshalIndent(plr.VSchema().Keyspaces, "", " ")
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
