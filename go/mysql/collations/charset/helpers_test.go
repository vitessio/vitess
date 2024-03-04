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

package charset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlice(t *testing.T) {
	s := Slice(Charset_binary{}, []byte("testString"), 1, 4)
	assert.Equal(t, []byte("est"), s)

	s = Slice(&testCharset1{}, []byte("testString"), 2, 5)
	assert.Equal(t, []byte("stS"), s)

	s = Slice(&testCharset1{}, []byte("testString"), 2, 20)
	assert.Equal(t, []byte("stString"), s)

	// Multibyte tests
	s = Slice(Charset_utf8mb4{}, []byte("😊😂🤢"), 1, 3)
	assert.Equal(t, []byte("😂🤢"), s)

	s = Slice(Charset_utf8mb4{}, []byte("😊😂🤢"), -2, 4)
	assert.Equal(t, []byte("😊😂🤢"), s)
}

func TestValidate(t *testing.T) {
	// TODO: Add more tests

	in := "testString"
	ok := Validate(Charset_binary{}, []byte(in))
	assert.True(t, ok, "'%s' should be validated from binary charset", in)

	ok = Validate(&testCharset1{}, nil)
	assert.True(t, ok, "Validate should return true for empty string irrespective of charset")

	ok = Validate(&testCharset1{}, []byte(in))
	assert.True(t, ok)
}

func TestLength(t *testing.T) {
	in := "testString"
	l := Length(Charset_binary{}, []byte(in))
	assert.Equal(t, 10, l)

	l = Length(&testCharset1{}, []byte(in))
	assert.Equal(t, 10, l)

	// Multibyte tests
	l = Length(Charset_utf8mb4{}, []byte("😊😂🤢"))
	assert.Equal(t, 3, l)

	l = Length(Charset_utf8mb4{}, []byte("한국어 시험"))
	assert.Equal(t, 6, l)
}
