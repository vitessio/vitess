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

type testCharset1 struct{}

func (c *testCharset1) Name() string {
	return "testCharset1"
}

func (c *testCharset1) SupportsSupplementaryChars() bool {
	return true
}

func (c *testCharset1) IsSuperset(other Charset) bool {
	return true
}

func (c *testCharset1) MaxWidth() int {
	return 1
}

func (c *testCharset1) EncodeRune([]byte, rune) int {
	return 0
}

func (c *testCharset1) DecodeRune(bytes []byte) (rune, int) {
	if len(bytes) < 1 {
		return RuneError, 0
	}
	return 1, 1
}

type testCharset2 struct{}

func (c *testCharset2) Name() string {
	return "testCharset2"
}

func (c *testCharset2) SupportsSupplementaryChars() bool {
	return true
}

func (c *testCharset2) IsSuperset(other Charset) bool {
	return false
}

func (c *testCharset2) MaxWidth() int {
	return 1
}

func (c *testCharset2) EncodeRune([]byte, rune) int {
	return 0
}

func (c *testCharset2) DecodeRune(bytes []byte) (rune, int) {
	if len(bytes) < 1 {
		return RuneError, 0
	}
	return rune(bytes[0]), 1
}

func (c *testCharset2) Convert(_, src []byte, from Charset) ([]byte, error) {
	return src, nil
}

func TestConvert(t *testing.T) {
	testCases := []struct {
		src        []byte
		srcCharset Charset
		dst        []byte
		dstCharset Charset
		want       []byte
		err        string
	}{
		{
			src:        []byte("testSrc"),
			srcCharset: Charset_utf8mb3{},
			dst:        []byte("testDst"),
			dstCharset: Charset_utf8mb4{},
			want:       []byte("testDsttestSrc"),
		},
		{
			src:        []byte("testSrc"),
			srcCharset: Charset_utf8mb3{},
			dst:        nil,
			dstCharset: Charset_utf8mb4{},
			want:       []byte("testSrc"),
		},
		{
			src:        []byte("testSrc"),
			srcCharset: Charset_utf8mb4{},
			dst:        nil,
			dstCharset: Charset_utf8mb3{},
			want:       []byte("testSrc"),
		},
		{
			src:        []byte("testSrc"),
			srcCharset: Charset_utf8mb4{},
			dst:        []byte("testDst"),
			dstCharset: Charset_utf8mb3{},
			want:       []byte("testDsttestSrc"),
		},
		{
			src:        []byte("😊😂🤢"),
			srcCharset: Charset_utf8mb4{},
			dst:        []byte("testDst"),
			dstCharset: Charset_utf8mb3{},
			want:       []byte("testDst???"),
			err:        "Cannot convert string",
		},
		{
			src:        []byte("testSrc"),
			srcCharset: Charset_binary{},
			dst:        []byte("testDst"),
			dstCharset: Charset_utf8mb3{},
			want:       []byte("testDsttestSrc"),
		},
		{
			src:        []byte{00, 65, 00, 66},
			srcCharset: Charset_ucs2{},
			dst:        []byte("testDst"),
			dstCharset: Charset_utf8mb3{},
			want:       []byte("testDstAB"),
		},
		{
			src:        []byte{00, 65, 00, 66},
			srcCharset: Charset_ucs2{},
			dst:        nil,
			dstCharset: Charset_utf8mb3{},
			want:       []byte("AB"),
		},
		{
			src:        []byte("😊😂🤢"),
			srcCharset: Charset_utf8mb3{},
			dst:        nil,
			dstCharset: &testCharset2{},
			want:       []byte("😊😂🤢"),
		},
	}

	for _, tc := range testCases {
		res, err := Convert(tc.dst, tc.dstCharset, tc.src, tc.srcCharset)

		if tc.err != "" {
			assert.ErrorContains(t, err, tc.err)
			assert.Equal(t, tc.want, res)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, res)
		}
	}
}

func TestExpand(t *testing.T) {
	testCases := []struct {
		dst        []rune
		src        []byte
		srcCharset Charset
		want       []rune
	}{
		{
			dst:        []rune("testDst"),
			src:        []byte("testSrc"),
			srcCharset: Charset_utf8mb3{},
			want:       []rune("testSrc"),
		},
		{
			dst:        nil,
			src:        []byte("testSrc"),
			srcCharset: Charset_utf8mb3{},
			want:       []rune("testSrc"),
		},
		{
			dst:        nil,
			src:        []byte("testSrc"),
			srcCharset: Charset_binary{},
			want:       []rune("testSrc"),
		},
		{
			dst:        []rune("testDst"),
			src:        []byte("testSrc"),
			srcCharset: Charset_binary{},
			want:       []rune("testDsttestSrc"),
		},
		{
			dst:        []rune("testDst"),
			src:        []byte{0, 0, 0, 0x41},
			srcCharset: Charset_utf32{},
			want:       []rune("testDstA"),
		},
		{
			dst:        nil,
			src:        []byte{0xFF},
			srcCharset: Charset_latin1{},
			want:       []rune("ÿ"),
		},
		// multibyte case
		{
			dst:        []rune("testDst"),
			src:        []byte("😊😂🤢"),
			srcCharset: Charset_utf8mb4{},
			want:       []rune("😊😂🤢"),
		},
	}

	for _, tc := range testCases {
		res := Expand(tc.dst, tc.src, tc.srcCharset)

		assert.Equal(t, tc.want, res)
	}
}

func TestCollapse(t *testing.T) {
	testCases := []struct {
		dst        []byte
		src        []rune
		dstCharset Charset
		want       []byte
	}{
		{
			dst:        []byte("testDst"),
			src:        []rune("testSrc"),
			dstCharset: Charset_utf8mb3{},
			want:       []byte("testDsttestSrc"),
		},
		{
			dst:        nil,
			src:        []rune("testSrc"),
			dstCharset: Charset_utf8mb3{},
			want:       []byte("testSrc"),
		},
		{
			dst:        []byte("testDst"),
			src:        []rune("testSrc"),
			dstCharset: Charset_utf8mb4{},
			want:       []byte("testDsttestSrc"),
		},
		{
			dst:        []byte("testDst"),
			src:        []rune("testSrc"),
			dstCharset: Charset_binary{},
			want:       []byte("testDsttestSrc"),
		},
		{
			dst:        nil,
			src:        []rune("testSrc"),
			dstCharset: Charset_binary{},
			want:       []byte("testSrc"),
		},
		{
			dst:        []byte("dst"),
			src:        []rune("src"),
			dstCharset: Charset_ucs2{},
			want:       []byte{100, 115, 116, 0, 115, 0, 114, 0, 99},
		},
		{
			dst:        nil,
			src:        []rune("src"),
			dstCharset: Charset_ucs2{},
			want:       []byte{0, 115, 0, 114, 0, 99},
		},
		// unsupported encoding case
		{
			dst:        nil,
			src:        []rune{0xffff1},
			dstCharset: Charset_ucs2{},
			want:       []byte{0, 63},
		},
	}

	for _, tc := range testCases {
		res := Collapse(tc.dst, tc.src, tc.dstCharset)

		assert.Equal(t, tc.want, res)
	}
}

func TestConvertFromUTF8(t *testing.T) {
	dst := []byte("dst")
	src := []byte("😊😂🤢")

	res, err := ConvertFromUTF8(dst, Charset_utf8mb4{}, src)
	assert.NoError(t, err)
	assert.Equal(t, []byte("dst😊😂🤢"), res)

	res, err = ConvertFromUTF8(dst, Charset_utf8mb3{}, src)
	assert.ErrorContains(t, err, "Cannot convert string")
	assert.Equal(t, []byte("dst???"), res)
}

func TestConvertFromBinary(t *testing.T) {
	testCases := []struct {
		dst  []byte
		cs   Charset
		in   []byte
		want []byte
		err  string
	}{
		{
			dst:  []byte("testDst"),
			cs:   Charset_utf8mb4{},
			in:   []byte("testString"),
			want: []byte("testDsttestString"),
		},
		{
			cs:   Charset_utf16le{},
			in:   []byte("testForOddLen"),
			want: append([]byte{0}, []byte("testForOddLen")...),
		},
		{
			cs:   Charset_utf16{},
			in:   []byte("testForEvenLen"),
			want: []byte("testForEvenLen"),
		},
		// multibyte case
		{
			dst:  []byte("testDst"),
			cs:   Charset_utf8mb4{},
			in:   []byte("😊😂🤢"),
			want: []byte("testDst😊😂🤢"),
		},
		// unsuppported encoding case
		{
			cs:  Charset_utf32{},
			in:  []byte{0xff},
			err: "Cannot convert string",
		},
	}

	for _, tc := range testCases {
		got, err := ConvertFromBinary(tc.dst, tc.cs, tc.in)

		if tc.want == nil {
			assert.ErrorContains(t, err, tc.err)
			assert.Nil(t, got)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		}
	}
}
