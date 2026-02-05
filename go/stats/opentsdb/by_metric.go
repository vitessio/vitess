/*
Copyright 2023 The Vitess Authors.

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

package opentsdb

// byMetric implements sort.Interface for []*DataPoint based on the metric key
// and then tag values (prioritized in tag name order). Having a consistent sort order
// is convenient when refreshing /debug/opentsdb or for encoding and comparing JSON directly
// in the tests.
type byMetric []*DataPoint

func (m byMetric) Len() int      { return len(m) }
func (m byMetric) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m byMetric) Less(i, j int) bool {
	if m[i].Metric < m[j].Metric {
		return true
	}

	if m[i].Metric > m[j].Metric {
		return false
	}

	// Metric names are the same. We can use tag values to figure out the sort order.
	// The deciding tag will be the lexicographically earliest tag name where tag values differ.
	decidingTagName := ""
	result := false
	for tagName, iVal := range m[i].Tags {
		jVal, ok := m[j].Tags[tagName]
		if !ok {
			// We'll arbitrarily declare that if i has any tag name that j doesn't then it sorts earlier.
			// This shouldn't happen in practice, though, if metric code is correct...
			return true
		}

		if iVal != jVal && (tagName < decidingTagName || decidingTagName == "") {
			decidingTagName = tagName
			result = iVal < jVal
		}
	}
	return result
}
