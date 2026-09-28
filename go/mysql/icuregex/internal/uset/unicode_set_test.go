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

package uset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleBelong(t *testing.T) {
	ss1 := New()
	ss1.AddString("*?+[(){}^$|\\.")
	ss2 := New()
	ss2.AddString("*?+[(){}^$|\\.")
	ss2.Complement()
	ss3 := New()
	ss3.AddRune('*')
	ss3.AddRune('?')

	assert.True(t, ss1.ContainsRune('('))
	assert.False(t, ss2.ContainsRune('('))
	assert.True(t, ss3.ContainsRune('*'))
}
