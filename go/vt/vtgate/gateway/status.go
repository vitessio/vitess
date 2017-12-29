/*
Copyright 2017 Google Inc.

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

package gateway

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/youtube/vitess/go/stats"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	aggrChanSize = 10000

	// StatusTemplate is the display part to use to show
	// a TabletCacheStatusList.
	StatusTemplate = `
<style>
  table {
    border-collapse: collapse;
  }
  td, th {
    border: 1px solid #999;
    padding: 0.2rem;
  }
  table tr:nth-child(even) {
    background-color: #eee;
  }
  table tr:nth-child(odd) {
    background-color: #fff;
  }
</style>
<table>
  <tr>
    <th>Keyspace</th>
    <th>Shard</th>
    <th>TabletType</th>
    <th>Address</th>
    <th>Query Sent</th>
    <th>Query Error</th>
    <th>QPS (avg 1m)</th>
    <th>Latency (ms) (avg 1m)</th>
  </tr>
  {{range $i, $status := .}}
  <tr>
    <td>{{$status.Keyspace}}</td>
    <td>{{$status.Shard}}</td>
    <td>{{$status.TabletType}}</td>
    <td><a href="http://{{$status.Addr}}">{{$status.Name}}</a></td>
    <td>{{$status.QueryCount}}</td>
    <td>{{$status.QueryError}}</td>
    <td>{{$status.FormattedQPS}}</td>
    <td>{{$status.AvgLatency}}</td>
  </tr>
  {{end}}
</table>
`
)

var (
	// aggrChan buffers queryInfo objects to be processed.
	aggrChan chan *queryInfo
	// muAggr protects below vars.
	muAggr sync.Mutex
	// aggregators holds all Aggregators created.
	aggregators []*TabletStatusAggregator
	// gatewayStatsChanFull tracks the number of times
	// aggrChan becomes full.
	gatewayStatsChanFull *stats.Int
)

func init() {
	// init global goroutines to aggregate stats.
	aggrChan = make(chan *queryInfo, aggrChanSize)
	gatewayStatsChanFull = stats.NewInt("GatewayStatsChanFullCount")
	go resetAggregators()
	go processQueryInfo()
}

// registerAggregator registers an aggregator to the global list.
func registerAggregator(a *TabletStatusAggregator) {
	muAggr.Lock()
	defer muAggr.Unlock()
	aggregators = append(aggregators, a)
}

// resetAggregators resets the next stats slot for all aggregators every second.
func resetAggregators() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		muAggr.Lock()
		for _, a := range aggregators {
			a.resetNextSlot()
		}
		muAggr.Unlock()
	}
}

// processQueryInfo processes the next queryInfo object.
func processQueryInfo() {
	for qi := range aggrChan {
		qi.aggr.processQueryInfo(qi)
	}
}

//
// TabletCacheStatus definitions
//

// TabletCacheStatus contains the status per destination for a gateway.
type TabletCacheStatus struct {
	Keyspace   string
	Shard      string
	TabletType topodatapb.TabletType
	Name       string
	Addr       string

	QueryCount uint64
	QueryError uint64
	QPS        float64
	AvgLatency float64 // in milliseconds
}

// FormattedQPS shows a 2 digit rounded value of QPS.
// Used in the HTML template above.
func (tcs *TabletCacheStatus) FormattedQPS() string {
	return fmt.Sprintf("%.2f", tcs.QPS)
}

//
// TabletStatusAggregator definitions
//

// TabletStatusAggregator tracks tablet status for a gateway.
type TabletStatusAggregator struct {
	Keyspace   string
	Shard      string
	TabletType topodatapb.TabletType
	Name       string // the alternative name of a tablet
	Addr       string // the host:port of a tablet

	// mu protects below fields.
	mu         sync.RWMutex
	QueryCount uint64
	QueryError uint64
	// for QPS and latency (avg value over a minute)
	tick               uint32
	queryCountInMinute [60]uint64
	latencyInMinute    [60]time.Duration
}

// queryInfo is sent over the aggregators channel to update the stats.
type queryInfo struct {
	aggr       *TabletStatusAggregator
	addr       string
	tabletType topodatapb.TabletType
	elapsed    time.Duration
	hasError   bool
}

// NewTabletStatusAggregator creates a TabletStatusAggregator.
func NewTabletStatusAggregator(keyspace, shard string, tabletType topodatapb.TabletType, name string) *TabletStatusAggregator {
	tsa := &TabletStatusAggregator{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
		Name:       name,
	}
	registerAggregator(tsa)
	return tsa
}

// UpdateQueryInfo updates the aggregator with the given information about a query.
func (tsa *TabletStatusAggregator) UpdateQueryInfo(addr string, tabletType topodatapb.TabletType, elapsed time.Duration, hasError bool) {
	qi := &queryInfo{
		aggr:       tsa,
		addr:       addr,
		tabletType: tabletType,
		elapsed:    elapsed,
		hasError:   hasError,
	}
	select {
	case aggrChan <- qi:
	default:
		gatewayStatsChanFull.Add(1)
	}
}

func (tsa *TabletStatusAggregator) processQueryInfo(qi *queryInfo) {
	tsa.mu.Lock()
	defer tsa.mu.Unlock()
	if tsa.TabletType != qi.tabletType {
		tsa.TabletType = qi.tabletType
		// reset counters
		tsa.QueryCount = 0
		tsa.QueryError = 0
		for i := 0; i < len(tsa.queryCountInMinute); i++ {
			tsa.queryCountInMinute[i] = 0
		}
		for i := 0; i < len(tsa.latencyInMinute); i++ {
			tsa.latencyInMinute[i] = 0
		}
	}
	if qi.addr != "" {
		tsa.Addr = qi.addr
	}
	tsa.QueryCount++
	tsa.queryCountInMinute[tsa.tick]++
	tsa.latencyInMinute[tsa.tick] += qi.elapsed
	if qi.hasError {
		tsa.QueryError++
	}
}

// GetCacheStatus returns a TabletCacheStatus representing the current gateway status.
func (tsa *TabletStatusAggregator) GetCacheStatus() *TabletCacheStatus {
	status := &TabletCacheStatus{
		Keyspace: tsa.Keyspace,
		Shard:    tsa.Shard,
		Name:     tsa.Name,
	}
	tsa.mu.RLock()
	defer tsa.mu.RUnlock()
	status.TabletType = tsa.TabletType
	status.Addr = tsa.Addr
	status.QueryCount = tsa.QueryCount
	status.QueryError = tsa.QueryError
	var totalQuery uint64
	for _, c := range tsa.queryCountInMinute {
		totalQuery += c
	}
	var totalLatency time.Duration
	for _, d := range tsa.latencyInMinute {
		totalLatency += d
	}
	status.QPS = float64(totalQuery) / 60
	if totalQuery > 0 {
		status.AvgLatency = float64(totalLatency.Nanoseconds()) / float64(totalQuery) / 1000000
	}
	return status
}

// resetNextSlot resets the next tracking slot.
func (tsa *TabletStatusAggregator) resetNextSlot() {
	tsa.mu.Lock()
	defer tsa.mu.Unlock()
	tsa.tick = (tsa.tick + 1) % 60
	tsa.queryCountInMinute[tsa.tick] = 0
	tsa.latencyInMinute[tsa.tick] = time.Duration(0)
}

//
// TabletCacheStatusList definitions
//

// TabletCacheStatusList is a slice of TabletCacheStatus.
type TabletCacheStatusList []*TabletCacheStatus

// Len is part of sort.Interface.
func (gtcsl TabletCacheStatusList) Len() int {
	return len(gtcsl)
}

// Less is part of sort.Interface.
func (gtcsl TabletCacheStatusList) Less(i, j int) bool {
	iKey := strings.Join([]string{gtcsl[i].Keyspace, gtcsl[i].Shard, string(gtcsl[i].TabletType), gtcsl[i].Name}, ".")
	jKey := strings.Join([]string{gtcsl[j].Keyspace, gtcsl[j].Shard, string(gtcsl[j].TabletType), gtcsl[j].Name}, ".")
	return iKey < jKey
}

// Swap is part of sort.Interface.
func (gtcsl TabletCacheStatusList) Swap(i, j int) {
	gtcsl[i], gtcsl[j] = gtcsl[j], gtcsl[i]
}
