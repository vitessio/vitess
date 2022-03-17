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
type nullCache[I any] struct{}

// Get never returns anything on the nullCache
func (n *nullCache[I]) Get(_ string) (i I, b bool) {
	return
}

// Set is a no-op in the nullCache
func (n *nullCache[I]) Set(_ string, _ I) bool {
	return false
}

// ForEach iterates the nullCache, which is always empty
func (n *nullCache[I]) ForEach(_ func(I) bool) {}

// Delete is a no-op in the nullCache
func (n *nullCache[I]) Delete(_ string) {}

// Clear is a no-op in the nullCache
func (n *nullCache[I]) Clear() {}

// Wait is a no-op in the nullcache
func (n *nullCache[I]) Wait() {}

func (n *nullCache[I]) Len() int {
	return 0
}

// Hits returns number of cache hits since creation
func (n *nullCache[I]) Hits() int64 {
	return 0
}

// Hits returns number of cache misses since creation
func (n *nullCache[I]) Misses() int64 {
	return 0
}

// Capacity returns the capacity of the nullCache, which is always 0
func (n *nullCache[I]) UsedCapacity() int64 {
	return 0
}

// Capacity returns the capacity of the nullCache, which is always 0
func (n *nullCache[I]) MaxCapacity() int64 {
	return 0
}

// SetCapacity sets the capacity of the null cache, which is a no-op
func (n *nullCache[I]) SetCapacity(_ int64) {}

func (n *nullCache[I]) Evictions() int64 {
	return 0
}
