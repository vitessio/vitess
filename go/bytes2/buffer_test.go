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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuffer(t *testing.T) {
	b := NewBuffer(nil)

	// Test Write function
	b.Write([]byte("ab"))
	assert.Equal(t, "ab", string(b.Bytes()), "Write()")

	// Test WriteString function
	b.WriteString("cd")
	assert.Equal(t, "abcd", string(b.Bytes()), "WriteString()")

	// Test WriteByte function
	b.WriteByte('e')
	assert.Equal(t, "abcde", string(b.Bytes()), "WriteByte()")

	// Test Bytes function
	assert.Equal(t, "abcde", string(b.Bytes()))

	// Test String function
	assert.Equal(t, "abcde", b.String())

	// Test StringUnsafe function
	assert.Equal(t, "abcde", b.StringUnsafe())

	// Test Len function
	assert.Equal(t, 5, b.Len())

	// Test Reset function
	b.Reset()
	assert.Equal(t, "", string(b.Bytes()))
	assert.Equal(t, 0, b.Len())
}
