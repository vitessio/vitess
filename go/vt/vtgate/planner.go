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

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/cache"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

// Planner is used to compute the plan. It contains
// the vschema, and has a cache of previous computed plans.
type Planner struct {
	vschema *planbuilder.VSchema
	plans   *cache.LRUCache
}

var once sync.Once

// NewPlanner creates a new planner for VTGate.
func NewPlanner(vschema *planbuilder.VSchema, cacheSize int) *Planner {
	plr := &Planner{
		vschema: vschema,
		plans:   cache.NewLRUCache(int64(cacheSize)),
	}
	once.Do(func() {
		http.Handle("/debug/query_plans", plr)
		http.Handle("/debug/vschema", plr)
	})
	return plr
}

// GetPlan computes the plan for the given query. If one is in
// the cache, it reuses it.
func (plr *Planner) GetPlan(sql string) (*planbuilder.Plan, error) {
	if plr.vschema == nil {
		return nil, errors.New("vschema not initialized")
	}
	if result, ok := plr.plans.Get(sql); ok {
		return result.(*planbuilder.Plan), nil
	}
	plan, err := planbuilder.BuildPlan(sql, plr.vschema)
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
		b, err := json.MarshalIndent(plr.vschema, "", " ")
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
