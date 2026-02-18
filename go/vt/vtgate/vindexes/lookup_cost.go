/*
Copyright 2024 The Vitess Authors.

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

package vindexes

import (
	"strconv"
)

var (
	_ SingleColumn   = (*LookupCost)(nil)
	_ Lookup         = (*LookupCost)(nil)
	_ LookupPlanable = (*LookupCost)(nil)
)

func init() {
	Register("lookup_cost", newLookupCost)
}

const defaultCost = 5

// LookupCost defines a test vindex that uses the cost provided by the user.
// This is a test vindex.
type LookupCost struct {
	*LookupNonUnique
	cost int
}

// Cost returns the cost of this vindex as provided.
func (lc *LookupCost) Cost() int {
	return lc.cost
}

func newLookupCost(name string, m map[string]string) (Vindex, error) {
	lookup, err := newLookup(name, m)
	if err != nil {
		return nil, err
	}
	cost := getInt(m, "cost", defaultCost)
	return &LookupCost{
		LookupNonUnique: lookup.(*LookupNonUnique),
		cost:            cost,
	}, nil
}

func getInt(m map[string]string, key string, defaultVal int) int {
	val, ok := m[key]
	if !ok {
		return defaultVal
	}
	intVal, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal
	}
	return intVal
}
