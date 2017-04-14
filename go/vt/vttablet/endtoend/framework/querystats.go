// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import (
	"encoding/json"
	"fmt"
	"net/http"
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
		return out
	}
	defer response.Body.Close()
	_ = json.NewDecoder(response.Body).Decode(&list)
	for _, stat := range list {
		out[stat.Query] = stat
	}
	return out
}
