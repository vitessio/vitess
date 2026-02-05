/*
© 2016 and later: Unicode, Inc. and others.
Copyright (C) 2004-2015, International Business Machines Corporation and others.
Copyright 2023 The Vitess Authors.

This file contains code derived from the Unicode Project's ICU library.
License & terms of use for the original code: http://www.unicode.org/copyright.html

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

package pattern

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnescapeAt(t *testing.T) {
	r, str := UnescapeAt("ud800\\ud800\\udc00")
	assert.Equal(t, rune(0xd800), r)
	assert.Equal(t, "\\ud800\\udc00", str)

	r, str = UnescapeAt(str[1:])
	assert.Equal(t, rune(0x00010000), r)
	assert.Equal(t, "", str)
}

func TestUnescapeAtRunes(t *testing.T) {
	r, str := UnescapeAtRunes([]rune("ud800\\ud800\\udc00"))
	assert.Equal(t, rune(0xd800), r)
	assert.Equal(t, []rune("\\ud800\\udc00"), str)

	r, str = UnescapeAtRunes(str[1:])
	assert.Equal(t, rune(0x00010000), r)
	assert.Equal(t, []rune(""), str)
}
