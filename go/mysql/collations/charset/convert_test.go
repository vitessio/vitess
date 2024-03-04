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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO: These types can be removed, use binary_charset instead.
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

func (c *testCharset2) DecodeRune([]byte) (rune, int) {
	return 1, 1
}

func TestConvert(t *testing.T) {
	dstCharset := &testCharset1{}
	srcCharset := &testCharset2{}
	src := []byte("src")

	res, err := Convert(nil, dstCharset, src, srcCharset)
	assert.NoError(t, err)
	assert.Equal(t, src, res)

	dst := []byte("dst")
	res, err = Convert(dst, dstCharset, src, srcCharset)
	assert.NoError(t, err)
	assert.Equal(t, []byte("dstsrc"), res)

	// TODO: Write more tests
	res, err = Convert(nil, &testCharset2{}, src, &testCharset1{})
	assert.NoError(t, err)
	fmt.Println(res)
}
