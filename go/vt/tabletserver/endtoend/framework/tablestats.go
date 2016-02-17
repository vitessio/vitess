// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// TableStat contains the stats for one table.
type TableStat struct {
	Hits, Absent, Misses, Invalidations int
}

// TableStats parses /debug/table_stats and returns
// a map of the table stats keyed by the table name.
func TableStats() map[string]TableStat {
	out := make(map[string]TableStat)
	response, err := http.Get(fmt.Sprintf("%s/debug/table_stats", ServerAddress))
	if err != nil {
		return out
	}
	defer response.Body.Close()
	_ = json.NewDecoder(response.Body).Decode(&out)
	return out
}
