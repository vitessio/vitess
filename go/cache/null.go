/*
Copyright 2021 The Vitess Authors.

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

package cache

// nullCache is a no-op cache that does not store items
type nullCache struct{}

// Get never returns anything on the nullCache
func (n *nullCache) Get(_ string) (interface{}, bool) {
	return nil, false
}

// Set is a no-op in the nullCache
func (n *nullCache) Set(_ string, _ interface{}, _ int64) {}

// ForEach iterates the nullCache, which is always empty
func (n *nullCache) ForEach(_ func(interface{}) bool) {}

// Delete is a no-op in the nullCache
func (n *nullCache) Delete(_ string) {}

// Clear is a no-op in the nullCache
func (n *nullCache) Clear() {}

// Stats returns a nil stats object for the nullCache
func (n *nullCache) Stats() *Stats {
	return &Stats{}
}

// Capacity returns the capacity of the nullCache, which is always 0
func (n *nullCache) Capacity() int64 {
	return 0
}

// SetCapacity sets the capacity of the null cache, which is a no-op
func (n *nullCache) SetCapacity(_ int64) {}
