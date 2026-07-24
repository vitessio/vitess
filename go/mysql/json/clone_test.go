/*
Copyright 2026 The Vitess Authors.

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

package json

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloneNilAndSingletons(t *testing.T) {
	var v *Value
	assert.Nil(t, v.Clone())

	// The boolean and null singletons are compared by pointer identity,
	// so Clone must preserve them.
	assert.Same(t, ValueTrue, ValueTrue.Clone())
	assert.Same(t, ValueFalse, ValueFalse.Clone())
	assert.Same(t, ValueNull, ValueNull.Clone())
}

func TestCloneMutationIndependence(t *testing.T) {
	t.Run("mutating the clone does not affect the original", func(t *testing.T) {
		original := MustParse(`{"a": [1, 2, 3], "o": {"x": [4, 5]}, "s": "keep"}`)
		want := original.String()

		clone := original.Clone()
		require.Equal(t, want, clone.String())

		obj, ok := clone.Object()
		require.True(t, ok)
		obj.Get("a").DelArrayItem(0)
		obj.Get("a").SetArrayItem(1, MustParse(`42`), Set)
		nested, ok := obj.Get("o").Object()
		require.True(t, ok)
		nested.Del("x")
		nested.Set("y", MustParse(`"new"`), Set)
		obj.Del("s")

		assert.Equal(t, `{"a": [2, 42], "o": {"y": "new"}}`, clone.String())
		assert.Equal(t, want, original.String())
	})

	t.Run("mutating the original does not affect the clone", func(t *testing.T) {
		original := MustParse(`{"a": [1, 2, 3], "o": {"x": [4, 5]}}`)
		clone := original.Clone()
		want := clone.String()

		obj, ok := original.Object()
		require.True(t, ok)
		obj.Get("a").DelArrayItem(2)
		obj.Set("b", MustParse(`[6]`), Set)
		nested, ok := obj.Get("o").Object()
		require.True(t, ok)
		nested.Set("x", MustParse(`7`), Replace)

		assert.Equal(t, `{"a": [1, 2], "b": [6], "o": {"x": 7}}`, original.String())
		assert.Equal(t, want, clone.String())
	})
}

func TestCloneKindPreservation(t *testing.T) {
	values := []*Value{
		NewNumber("1.5", NumberTypeDecimal),
		NewNumber("18446744073709551615", NumberTypeUnsigned),
		NewNumber("-42", NumberTypeSigned),
		NewNumber("1.5e10", NumberTypeFloat),
		NewString("foo"),
		NewBlob("\x00\x01"),
		NewBit("\x81"),
		NewDate("2023-10-12"),
		NewTime("14:35:02"),
		NewDateTime("2023-10-12 14:35:02"),
		NewOpaqueValue("opaque"),
		ValueTrue,
		ValueFalse,
		ValueNull,
	}
	original := NewArray(values)
	clone := original.Clone()

	cloned, ok := clone.Array()
	require.True(t, ok)
	require.Len(t, cloned, len(values))
	for i, v := range values {
		assert.Equal(t, v.Type(), cloned[i].Type())
		assert.Equal(t, v.NumberType(), cloned[i].NumberType())
		assert.Equal(t, v.Raw(), cloned[i].Raw())
	}
}

func TestCloneRawStrings(t *testing.T) {
	// Type lazily unescapes raw strings by rewriting their backing bytes in
	// place; the clone must own its bytes so that unescaping one value
	// cannot corrupt the other.
	original := MustParse(`["fo\no"]`)
	clone := original.Clone()

	cloned, ok := clone.Array()
	require.True(t, ok)
	s, ok := cloned[0].StringBytes()
	require.True(t, ok)
	assert.Equal(t, "fo\no", string(s))

	arr, ok := original.Array()
	require.True(t, ok)
	s, ok = arr[0].StringBytes()
	require.True(t, ok)
	assert.Equal(t, "fo\no", string(s))
}
