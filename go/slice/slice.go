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

// Package slice contains generic Slice helpers;
// Some of this code is sourced from https://github.com/luraim/fun (Apache v2)
package slice

// All returns true if all elements return true for given predicate
func All[T any](s []T, fn func(T) bool) bool {
	for _, e := range s {
		if !fn(e) {
			return false
		}
	}
	return true
}

// Any returns true if at least one element returns true for given predicate
func Any[T any](s []T, fn func(T) bool) bool {
	for _, e := range s {
		if fn(e) {
			return true
		}
	}
	return false
}

// Map applies a function to each element of a slice and returns a new slice
func Map[From, To any](in []From, f func(From) To) []To {
	if in == nil {
		return nil
	}
	result := make([]To, len(in))
	for i, col := range in {
		result[i] = f(col)
	}
	return result
}

// MapWithError applies a function to each element of a slice and returns a new slice, or an error
func MapWithError[From, To any](in []From, f func(From) (To, error)) (result []To, err error) {
	if in == nil {
		return nil, nil
	}
	result = make([]To, len(in))
	for i, col := range in {
		result[i], err = f(col)
		if err != nil {
			return nil, err
		}
	}
	return
}

// Filter returns a new slice containing only the elements for which the predicate returns true
func Filter[T any](in []T, f func(T) bool) []T {
	if in == nil {
		return nil
	}
	result := make([]T, 0, len(in))
	for _, col := range in {
		if f(col) {
			result = append(result, col)
		}
	}
	return result
}
