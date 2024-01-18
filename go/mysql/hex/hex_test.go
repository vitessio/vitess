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

package hex

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeBytes(t *testing.T) {
	testCases := []struct {
		input []byte
		want  []byte
	}{
		{[]byte{0xAB, 0xCD, 0xEF}, []byte("ABCDEF")},
		{[]byte{0x01, 0x23, 0x45}, []byte("012345")},
	}

	for _, tCase := range testCases {
		got := EncodeBytes(tCase.input)
		assert.Equal(t, tCase.want, got)
	}
}

func TestEncodeUint(t *testing.T) {
	testCases := []struct {
		input uint64
		want  []byte
	}{
		{0, []byte("0")},
		{123, []byte("7B")},
		{255, []byte("FF")},
		{4096, []byte("1000")},
	}

	for _, tCase := range testCases {
		got := EncodeUint(tCase.input)
		assert.Equal(t, tCase.want, got)
	}
}

func TestDecodeUint(t *testing.T) {
	testCases := []struct {
		input uint64
		want  []byte
	}{
		{0, []byte{0}},
		{123, []byte{0x01, 0x23}},
		{255, []byte{0x02, 0x55}},
		{4096, []byte{0x40, 0x96}},
	}

	for _, tCase := range testCases {
		got := DecodeUint(tCase.input)
		assert.Equal(t, tCase.want, got)
	}
}

func TestDecodedLen(t *testing.T) {
	testCases := []struct {
		input []byte
		want  int
	}{
		{[]byte{0}, 1},
		{[]byte{0x01, 0x23}, 1},
		{[]byte("ABCDE"), 3},
		{[]byte("0123456789ABCDEF"), 8},
	}

	for _, tCase := range testCases {
		got := DecodedLen(tCase.input)
		assert.Equal(t, tCase.want, got)
	}
}

func TestDecodeBytes(t *testing.T) {
	err := DecodeBytes([]byte("testDst"), []byte("1"))
	assert.NoError(t, err)

	err = DecodeBytes([]byte("testDst"), []byte("12"))
	assert.NoError(t, err)

	// DecodeBytes should return an error for "é" as
	// hex.decode returns an error for non-ASCII characters
	err = DecodeBytes([]byte("testDst"), []byte("é"))
	assert.Error(t, err)
}
