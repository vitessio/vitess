/*
Copyright 2022 The Vitess Authors.

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

package charset

func Slice(charset Charset, input []byte, from, to int) []byte {
	if charset, ok := charset.(interface{ Slice([]byte, int, int) []byte }); ok {
		return charset.Slice(input, from, to)
	}
	iter := input
	for i := 0; i < to; i++ {
		r, size := charset.DecodeRune(iter)
		if r == RuneError && size < 2 {
			break
		}
		iter = iter[size:]
	}
	return input[:len(input)-len(iter)]
}

func Validate(charset Charset, input []byte) bool {
	if charset, ok := charset.(interface{ Validate([]byte) bool }); ok {
		return charset.Validate(input)
	}
	for len(input) > 0 {
		r, size := charset.DecodeRune(input)
		if r == RuneError && size < 2 {
			return false
		}
		input = input[size:]
	}
	return true
}

func Length(charset Charset, input []byte) int {
	if charset, ok := charset.(interface{ Length([]byte) int }); ok {
		return charset.Length(input)
	}
	var count int
	for len(input) > 0 {
		_, size := charset.DecodeRune(input)
		input = input[size:]
		count++
	}
	return count
}
