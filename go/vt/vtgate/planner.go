// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

// This is a V3 file. Do not intermix with V2.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/henryanand/vitess/go/acl"
	"github.com/henryanand/vitess/go/cache"
	"github.com/henryanand/vitess/go/vt/vtgate/planbuilder"
)

var noPlan = &planbuilder.Plan{
	ID:     planbuilder.NoPlan,
	Reason: "planbuiler not initialized",
}

type Planner struct {
	schema *planbuilder.Schema
	plans  *cache.LRUCache
}

func NewPlanner(schema *planbuilder.Schema, cacheSize int) *Planner {
	plr := &Planner{
		schema: schema,
		plans:  cache.NewLRUCache(int64(cacheSize)),
	}
	// TODO(sougou): Uncomment after making Planner testable.
	//http.Handle("/debug/query_plans", plr)
	//http.Handle("/debug/schema", plr)
	return plr
}

func (plr *Planner) GetPlan(sql string) *planbuilder.Plan {
	if plr.schema == nil {
		return noPlan
	}
	if result, ok := plr.plans.Get(sql); ok {
		return result.(*planbuilder.Plan)
	}
	plan := planbuilder.BuildPlan(sql, plr.schema)
	plr.plans.Set(sql, plan)
	return plan
}

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
			if plan, ok := plr.plans.Get(v); ok {
				if b, err := json.MarshalIndent(plan, "", "  "); err != nil {
					response.Write([]byte(err.Error()))
				} else {
					response.Write(b)
				}
				response.Write(([]byte)("\n\n"))
			}
		}
	} else if request.URL.Path == "/debug/schema" {
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		b, err := json.MarshalIndent(plr.schema, "", " ")
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
