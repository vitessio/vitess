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

package framework

import (
	"encoding/json"
	"fmt"
	"net/http"

	"vitess.io/vitess/go/vt/log"
)

// QueryStat contains the stats for one query.
type QueryStat struct {
	Query, Table, Plan                                string
	QueryCount, Time, MysqlTime, RowCount, ErrorCount int
}

// QueryStats parses /debug/query_stats and returns
// a map of the query stats keyed by the query.
func QueryStats() map[string]QueryStat {
	out := make(map[string]QueryStat)
	var list []QueryStat
	response, err := http.Get(fmt.Sprintf("%s/debug/query_stats", ServerAddress))
	if err != nil {
		log.Warning("failed to read query stats")
		return out
	}
	defer func() { _ = response.Body.Close() }()
	_ = json.NewDecoder(response.Body).Decode(&list)
	for _, stat := range list {
		out[stat.Query] = stat
	}
	return out
}
