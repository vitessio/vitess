/*
   Copyright 2014 Shlomi Noach.

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

package math

func MinInt(i1, i2 int) int {
	if i1 < i2 {
		return i1
	}
	return i2
}

func MaxInt(i1, i2 int) int {
	if i1 > i2 {
		return i1
	}
	return i2
}

func MinInt64(i1, i2 int64) int64 {
	if i1 < i2 {
		return i1
	}
	return i2
}

func MaxInt64(i1, i2 int64) int64 {
	if i1 > i2 {
		return i1
	}
	return i2
}

func MinUInt(i1, i2 uint) uint {
	if i1 < i2 {
		return i1
	}
	return i2
}

func MaxUInt(i1, i2 uint) uint {
	if i1 > i2 {
		return i1
	}
	return i2
}

func MinUInt64(i1, i2 uint64) uint64 {
	if i1 < i2 {
		return i1
	}
	return i2
}

func MaxUInt64(i1, i2 uint64) uint64 {
	if i1 > i2 {
		return i1
	}
	return i2
}

func MinString(i1, i2 string) string {
	if i1 < i2 {
		return i1
	}
	return i2
}

func MaxString(i1, i2 string) string {
	if i1 > i2 {
		return i1
	}
	return i2
}

// TernaryString acts like a "? :" C-style ternary operator for strings
func TernaryString(condition bool, resTrue string, resFalse string) string {
	if condition {
		return resTrue
	}
	return resFalse
}

// TernaryString acts like a "? :" C-style ternary operator for ints
func TernaryInt(condition bool, resTrue int, resFalse int) int {
	if condition {
		return resTrue
	}
	return resFalse
}

// AbsInt is an ABS function for int type
func AbsInt(i int) int {
	if i >= 0 {
		return i
	}
	return -i
}

// AbsInt64 is an ABS function for int64 type
func AbsInt64(i int64) int64 {
	if i >= 0 {
		return i
	}
	return -i
}
