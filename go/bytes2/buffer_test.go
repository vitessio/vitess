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

package bytes2

import "testing"

// Checks if the actual value is equal to the expected value for the given function
func assertEqual(t *testing.T, got any, want any, funcName string) {
	if got != want {
		t.Errorf("%s: got %v, want %v", funcName, got, want)
	}
}

func TestBuffer(t *testing.T) {
	// Initialize a new buffer
	b := NewBuffer(nil)

	// Test Write function
	b.Write([]byte("ab"))
	assertEqual(t, string(b.Bytes()), "ab", "Write()")

	// Test WriteString function
	b.WriteString("cd")
	assertEqual(t, string(b.Bytes()), "abcd", "WriteString()")

	// Test WriteByte function
	b.WriteByte('e')
	assertEqual(t, string(b.Bytes()), "abcde", "WriteByte()")

	// Test Bytes function
	assertEqual(t, string(b.Bytes()), "abcde", "Bytes()")

	// Test String function
	assertEqual(t, b.String(), "abcde", "String()")

	// Test StringUnsafe function
	assertEqual(t, b.StringUnsafe(), "abcde", "StringUnsafe()")

	// Test Len function
	assertEqual(t, b.Len(), 5, "Len()")

	// Test Reset function
	b.Reset()
	assertEqual(t, string(b.Bytes()), "", "Reset()")
	assertEqual(t, b.Len(), 0, "Reset() - Len()")
}
