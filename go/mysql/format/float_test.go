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

package format

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatFloat(t *testing.T) {
	testCases := []struct {
		input float64
		want  []byte
	}{
		{123.456, []byte("123.456")},
		{-1.13456e15, []byte("-1.13456e15")},
		{2e15, []byte("2e15")},
		{2e-15, []byte("0.000000000000002")},
		{-1e-16, []byte("-1e-16")},
		{0.0, []byte("0")},
	}

	for _, tCase := range testCases {
		got := FormatFloat(tCase.input)
		assert.Equal(t, tCase.want, got)
	}
}
