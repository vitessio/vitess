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

package ptr

// Of returns a pointer to the given value
func Of[T any](x T) *T {
	return &x
}

// Unwrap dereferences the given pointer if it's not nil.
// Otherwise, it returns default_
func Unwrap[T any](x *T, default_ T) T {
	if x != nil {
		return *x
	}
	return default_
}
