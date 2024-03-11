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
	testCases := []struct {
		in   []byte
		cs   Charset
		from int
		to   int
		want []byte
	}{
		{
			in:   []byte("testString"),
			cs:   Charset_binary{},
			from: 1,
			to:   4,
			want: []byte("est"),
		},
		{
			in:   []byte("testString"),
			cs:   &testCharset1{},
			from: 2,
			to:   5,
			want: []byte("stS"),
		},
		{
			in:   []byte("testString"),
			cs:   &testCharset1{},
			from: 2,
			to:   20,
			want: []byte("stString"),
		},
		// Multibyte cases
		{
			in:   []byte("😊😂🤢"),
			cs:   Charset_utf8mb4{},
			from: 1,
			to:   3,
			want: []byte("😂🤢"),
		},
		{
			in:   []byte("😊😂🤢"),
			cs:   Charset_utf8mb4{},
			from: -2,
			to:   4,
			want: []byte("😊😂🤢"),
		},
	}

	for _, tc := range testCases {
		s := Slice(tc.cs, tc.in, tc.from, tc.to)
		assert.Equal(t, tc.want, s)
	}
}

func TestValidate(t *testing.T) {
	in := "testString"
	ok := Validate(Charset_binary{}, []byte(in))
	assert.True(t, ok, "%q should be valid for binary charset", in)

	ok = Validate(&testCharset1{}, nil)
	assert.True(t, ok, "Validate should return true for empty string irrespective of charset")

	ok = Validate(&testCharset1{}, []byte(in))
	assert.True(t, ok, "%q should be valid for testCharset1", in)

	ok = Validate(Charset_utf16le{}, []byte{0x41})
	assert.False(t, ok, "%v should not be valid for utf16le charset", []byte{0x41})
}

func TestLength(t *testing.T) {
	testCases := []struct {
		in   []byte
		cs   Charset
		want int
	}{
		{[]byte("testString"), Charset_binary{}, 10},
		{[]byte("testString"), &testCharset1{}, 10},
		// Multibyte cases
		{[]byte("😊😂🤢"), Charset_utf8mb4{}, 3},
		{[]byte("한국어 시험"), Charset_utf8mb4{}, 6},
	}

	for _, tc := range testCases {
		l := Length(tc.cs, tc.in)
		assert.Equal(t, tc.want, l)
	}
}
