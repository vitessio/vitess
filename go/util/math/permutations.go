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

/*
Adapted from https://yourbasic.org/golang/generate-permutation-slice-string/
Licensed under https://creativecommons.org/licenses/by/3.0/
Modified to permutate an auto-generated slice of indexes
*/

package mathutil

// Permutations calls callback with each permutation of a slice of indexes 0..n-1
func Permutations(n int, callback func([]int)) {
	ints := []int{}
	for i := 0; i < n; i++ {
		ints = append(ints, i)
	}
	perm(ints, callback, 0)
}

// Permute the values at index i to len(a)-1.
func perm(a []int, callback func([]int), i int) {
	if i > len(a) {
		callback(a)
		return
	}
	perm(a, callback, i+1)
	for j := i + 1; j < len(a); j++ {
		a[i], a[j] = a[j], a[i]
		perm(a, callback, i+1)
		a[i], a[j] = a[j], a[i]
	}
}
