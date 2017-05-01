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
	"sort"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/cache"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"

	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// Planner is used to compute the plan. It contains
// the vschema, and has a cache of previous computed plans.
type Planner struct {
	serv         topo.SrvTopoServer
	cell         string
	mu           sync.Mutex
	vschema      *vindexes.VSchema
	normalize    bool
	plans        *cache.LRUCache
	vschemaStats *VSchemaStats
}

// VSchemaStats contains a rollup of the VSchema stats.
type VSchemaStats struct {
	Error     string
	Keyspaces VSchemaKeyspaceStatsList
}

// VSchemaKeyspaceStats contains a rollup of the VSchema stats for a keyspace.
// It is used to display a table with the information in the status page.
type VSchemaKeyspaceStats struct {
	Keyspace    string
	Sharded     bool
	TableCount  int
	VindexCount int
}

// VSchemaKeyspaceStatsList is to sort VSchemaKeyspaceStats by keyspace.
type VSchemaKeyspaceStatsList []*VSchemaKeyspaceStats

// Len is part of sort.Interface
func (l VSchemaKeyspaceStatsList) Len() int {
	return len(l)
}

// Less is part of sort.Interface
func (l VSchemaKeyspaceStatsList) Less(i, j int) bool {
	return l[i].Keyspace < l[j].Keyspace
}

// Swap is part of sort.Interface
func (l VSchemaKeyspaceStatsList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// NewVSchemaStats returns a new VSchemaStats from a VSchema.
func NewVSchemaStats(vschema *vindexes.VSchema, errorMessage string) *VSchemaStats {
	stats := &VSchemaStats{
		Error:     errorMessage,
		Keyspaces: make([]*VSchemaKeyspaceStats, 0, len(vschema.Keyspaces)),
	}
	for n, k := range vschema.Keyspaces {
		s := &VSchemaKeyspaceStats{
			Keyspace: n,
		}
		if k.Keyspace != nil {
			s.Sharded = k.Keyspace.Sharded
			s.TableCount += len(k.Tables)
			for _, t := range k.Tables {
				s.VindexCount += len(t.ColumnVindexes) + len(t.Ordered) + len(t.Owned)
			}
		}
		stats.Keyspaces = append(stats.Keyspaces, s)
	}
	sort.Sort(stats.Keyspaces)

	return stats
}

const (
	// VSchemaTemplate is the HTML template to display VSchemaStats.
	VSchemaTemplate = `
<style>
  table {
    border-collapse: collapse;
  }
  td, th {
    border: 1px solid #999;
    padding: 0.2rem;
  }
</style>
<table>
  <tr>
    <th colspan="4">VSchema{{if not .Error}} <i><a href="/debug/vschema">in JSON</a></i>{{end}}</th>
  </tr>
{{if .Error}}
  <tr>
    <th>Error</th>
    <td colspan="3">{{$.Error}}</td>
  </tr>
{{else}}
  <tr>
    <th>Keyspace</th>
    <th>Sharded</th>
    <th>Table Count</th>
    <th>Vindex Count</th>
  </tr>
{{range $i, $ks := .Keyspaces}}  <tr>
    <td>{{$ks.Keyspace}}</td>
    <td>{{if $ks.Sharded}}Yes{{else}}No{{end}}</td>
    <td>{{$ks.TableCount}}</td>
    <td>{{$ks.VindexCount}}</td>
  </tr>{{end}}
{{end}}
</table>
`
)

var plannerOnce sync.Once

// NewPlanner creates a new planner for VTGate.
// It will watch the vschema in the topology until the ctx is closed.
func NewPlanner(ctx context.Context, serv topo.SrvTopoServer, cell string, cacheSize int, normalize bool) *Planner {
	plr := &Planner{
		serv:      serv,
		cell:      cell,
		plans:     cache.NewLRUCache(int64(cacheSize)),
		normalize: normalize,
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
		saveVSchema := func(v *vschemapb.SrvVSchema, errorMessage string) {
			// transform the provided SrvVSchema into a VSchema
			var vschema *vindexes.VSchema
			if v != nil {
				var err error
				vschema, err = vindexes.BuildVSchema(v)
				if err != nil {
					log.Warningf("Error creating VSchema for cell %v (will try again next update): %v", cell, err)
					v = nil
					errorMessage = fmt.Sprintf("Error creating VSchema for cell %v: %v", cell, err)
					if vschemaCounters != nil {
						vschemaCounters.Add("Parsing", 1)
					}
				}
			}
			if v == nil {
				// we encountered an error, build an
				// empty vschema
				vschema, _ = vindexes.BuildVSchema(&vschemapb.SrvVSchema{})
			}

			// Build the display version.
			stats := NewVSchemaStats(vschema, errorMessage)

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
			plr.vschemaStats = stats
			plr.mu.Unlock()
			plr.plans.Clear()

			if vschemaCounters != nil {
				vschemaCounters.Add("Reload", 1)
			}

			// notify the listener
			if !foundFirstValue {
				foundFirstValue = true
				wg.Done()
			}
		}

		for {
			current, changes, _ := plr.serv.WatchSrvVSchema(ctx, cell)
			if current.Err != nil {
				// Don't log if there is no VSchema to start with.
				if current.Err != topo.ErrNoNode {
					log.Warningf("Error watching vschema for cell %s (will wait 5s before retrying): %v", cell, current.Err)
				}
				saveVSchema(nil, fmt.Sprintf("Error watching SvrVSchema: %v", current.Err.Error()))
				if vschemaCounters != nil {
					vschemaCounters.Add("WatchError", 1)
				}
				time.Sleep(5 * time.Second)
				continue
			}
			saveVSchema(current.Value, "")

			for c := range changes {
				if c.Err != nil {
					// If the SrvVschema disappears, we need to clear our record.
					// Otherwise, keep what we already had before.
					if c.Err == topo.ErrNoNode {
						saveVSchema(nil, "SrvVSchema object was removed from topology.")
					}
					log.Warningf("Error while watching vschema for cell %s (will wait 5s before retrying): %v", cell, c.Err)
					if vschemaCounters != nil {
						vschemaCounters.Add("WatchError", 1)
					}
					break
				}
				saveVSchema(c.Value, "")
			}

			// Sleep a bit before trying again.
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
func (plr *Planner) GetPlan(sql, keyspace string, bindvars map[string]interface{}) (*engine.Plan, error) {
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
	if !plr.normalize {
		plan, err := planbuilder.Build(sql, &wrappedVSchema{
			vschema:  plr.VSchema(),
			keyspace: sqlparser.NewTableIdent(keyspace),
		})
		if err != nil {
			return nil, err
		}
		plr.plans.Set(key, plan)
		return plan, nil
	}
	// Normalize and retry.
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	sqlparser.Normalize(stmt, bindvars, "vtg")
	normalized := sqlparser.String(stmt)
	normkey := normalized
	if keyspace != "" {
		normkey = keyspace + ":" + normalized
	}
	if result, ok := plr.plans.Get(normkey); ok {
		return result.(*engine.Plan), nil
	}
	plan, err := planbuilder.BuildFromStmt(normalized, stmt, &wrappedVSchema{
		vschema:  plr.VSchema(),
		keyspace: sqlparser.NewTableIdent(keyspace),
	})
	if err != nil {
		return nil, err
	}
	plr.plans.Set(normkey, plan)
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

// VSchemaStats returns the loaded vschema stats.
func (plr *Planner) VSchemaStats() *VSchemaStats {
	plr.mu.Lock()
	defer plr.mu.Unlock()
	if plr.vschemaStats == nil {
		return &VSchemaStats{
			Error: "No VSchema loaded yet.",
		}
	}
	return plr.vschemaStats
}

type wrappedVSchema struct {
	vschema  *vindexes.VSchema
	keyspace sqlparser.TableIdent
}

func (vs *wrappedVSchema) Find(keyspace, tablename sqlparser.TableIdent) (table *vindexes.Table, err error) {
	if keyspace.IsEmpty() {
		keyspace = vs.keyspace
	}
	return vs.vschema.Find(keyspace.String(), tablename.String())
}
