/*
Copyright 2019 The Vitess Authors.

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

package splitquery

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

type tuple []sqltypes.Value

// SplitAlgorithmInterface defines the interface for a splitting algorithm.
type SplitAlgorithmInterface interface {
	// generateBoundaries() method should return a list of "boundary tuples".
	// Each tuple is expected to contain values of the split columns, in the order these are returned
	// in getSplitColumns(), at a specific "boundary point". The returned list should be
	// ordered using ascending lexicographical order.
	// If the resulting list of boundary tuples is: {t1, t2, ..., t_k}, the
	// splitquery.Splitter.Split() method would generate k+1 query parts.
	// For i=0,1,...,k, the ith query-part contains the rows whose tuple of split-column values 't'
	// satisfies t_i < t <= t+1, where the comparison is performed lexicographically, and t_0 and
	// t_k+1 are taken to be a "-infinity" tuple and a "+infinity" tuple, respectively.
	generateBoundaries() ([]tuple, error)

	// getSplitColumns() should return the list of split-columns used by the algorithm.
	getSplitColumns() []*schema.TableColumn
}
