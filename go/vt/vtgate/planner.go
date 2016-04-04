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
func NewPlanner(ctx context.Context, serv topo.SrvTopoServer, cell string, cacheSize int) *Planner {
	plr := &Planner{
		serv:  serv,
		cell:  cell,
		plans: cache.NewLRUCache(int64(cacheSize)),
	}
	plr.LoadVSchema(ctx)
	once.Do(func() {
		http.Handle("/debug/query_plans", plr)
		http.Handle("/debug/vschema", plr)
	})
	return plr
}

// LoadVSchema loads the VSchema from the topo. The function does
// not return an error. It instead logs warnings on failure.
func (plr *Planner) LoadVSchema(ctx context.Context) {
	formal := &vindexes.VSchemaFormal{
		Keyspaces: make(map[string]vindexes.KeyspaceFormal),
	}
	keyspaces, err := plr.serv.GetSrvKeyspaceNames(ctx, plr.cell)
	if err != nil {
		log.Warningf("Error loading vschema: could not read keyspaces: %v", err)
		return
	}
	for _, keyspace := range keyspaces {
		formal.Keyspaces[keyspace] = vindexes.KeyspaceFormal{}
		kschema, err := plr.serv.GetVSchema(context.TODO(), keyspace)
		if err != nil {
			log.Warningf("Error loading vschema for keyspace: %s: %v", keyspace, err)
			continue
		}
		var kformal vindexes.KeyspaceFormal
		err = json.Unmarshal([]byte(kschema), &kformal)
		if err != nil {
			log.Warningf("Error unmarshalling vschema for keyspace: %s: %v", keyspace, err)
			continue
		}
		formal.Keyspaces[keyspace] = kformal
	}
	vschema, err := vindexes.BuildVSchema(formal)
	if err != nil {
		log.Warningf("Error creating VSchema: %v", err)
		return
	}
	plr.mu.Lock()
	plr.vschema = vschema
	plr.mu.Unlock()
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
