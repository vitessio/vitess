/*
Copyright 2022 The Kubernetes Authors.

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

package sets

import "sort"

// ordered is used to constraint the set to be only for
// some basic ordered types. We only care about string here
// for now.
type ordered interface {
	~string
}

type Set[T comparable] map[T]struct{}

// New creates a Set from a list of values.
func New[T comparable](items ...T) Set[T] {
	s := make(Set[T], len(items))
	s.Insert(items...)
	return s
}

// Insert adds items to the set.
func (s Set[T]) Insert(items ...T) Set[T] {
	for _, item := range items {
		s[item] = struct{}{}
	}
	return s
}

// Delete removes all items from the set.
func (s Set[T]) Delete(items ...T) Set[T] {
	for _, item := range items {
		delete(s, item)
	}
	return s
}

// Has returns true if and only if item is contained in the set.
func (s Set[T]) Has(item T) bool {
	_, found := s[item]
	return found
}

// HasAny returns true if any items are contained in the set.
func (s Set[T]) HasAny(items ...T) bool {
	for _, item := range items {
		if s.Has(item) {
			return true
		}
	}
	return false
}

// Difference returns a set of objects that are not in other.
func (s Set[T]) Difference(other Set[T]) Set[T] {
	result := New[T]()
	for key := range s {
		if !other.Has(key) {
			result.Insert(key)
		}
	}
	return result
}

// Intersection returns a new set which includes the item in BOTH s and other
func (s Set[T]) Intersection(o Set[T]) Set[T] {
	var walk, other Set[T]
	result := New[T]()
	if s.Len() < o.Len() {
		walk = s
		other = o
	} else {
		walk = o
		other = s
	}
	for key := range walk {
		if other.Has(key) {
			result.Insert(key)
		}
	}
	return result
}

// Equal returns if both sets contain the same elements
func (s Set[T]) Equal(other Set[T]) bool {
	return len(s) == len(other) && len(s) == len(s.Intersection(other))
}

type sortableSlice[T ordered] []T

func (g sortableSlice[T]) Len() int           { return len(g) }
func (g sortableSlice[T]) Less(i, j int) bool { return less[T](g[i], g[j]) }
func (g sortableSlice[T]) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }

// List returns the contents as a sorted T slice.
//
// This is a separate function and not a method because not all types supported
// by Generic are ordered and only those can be sorted.
func List[T ordered](s Set[T]) []T {
	res := make(sortableSlice[T], 0, len(s))
	for key := range s {
		res = append(res, key)
	}
	sort.Sort(res)
	return res
}

// Len returns the size of the set.
func (s Set[T]) Len() int {
	return len(s)
}

func less[T ordered](lhs, rhs T) bool {
	return lhs < rhs
}
