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

package visitorgen

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSingleItem(t *testing.T) {
	sfi := SingleFieldItem{
		StructType: &Ref{&TypeString{"Struct"}},
		FieldType:  &TypeString{"string"},
		FieldName:  "Field",
	}

	expectedReplacer := `func replaceStructField(newNode, parent SQLNode) {
	parent.(*Struct).Field = newNode.(string)
}`

	expectedSwitch := `		a.apply(node, n.Field, replaceStructField)`
	require.Equal(t, expectedReplacer, sfi.asReplMethod())
	require.Equal(t, expectedSwitch, sfi.asSwitchCase())
}

func TestArrayFieldItem(t *testing.T) {
	sfi := ArrayFieldItem{
		StructType: &Ref{&TypeString{"Struct"}},
		ItemType:   &TypeString{"string"},
		FieldName:  "Field",
	}

	expectedReplacer := `type replaceStructField int

func (r *replaceStructField) replace(newNode, container SQLNode) {
	container.(*Struct).Field[int(*r)] = newNode.(string)
}

func (r *replaceStructField) inc() {
	*r++
}`

	expectedSwitch := `		replacerField := replaceStructField(0)
		replacerFieldB := &replacerField
		for _, item := range n.Field {
			a.apply(node, item, replacerFieldB.replace)
			replacerFieldB.inc()
		}`
	require.Equal(t, expectedReplacer, sfi.asReplMethod())
	require.Equal(t, expectedSwitch, sfi.asSwitchCase())
}

func TestArrayItem(t *testing.T) {
	sfi := ArrayItem{
		StructType: &Ref{&TypeString{"Struct"}},
		ItemType:   &TypeString{"string"},
	}

	expectedReplacer := `type replaceStructItems int

func (r *replaceStructItems) replace(newNode, container SQLNode) {
	container.(*Struct)[int(*r)] = newNode.(string)
}

func (r *replaceStructItems) inc() {
	*r++
}`

	expectedSwitch := `		replacer := replaceStructItems(0)
		replacerRef := &replacer
		for _, item := range n {
			a.apply(node, item, replacerRef.replace)
			replacerRef.inc()
		}`
	require.Equal(t, expectedReplacer, sfi.asReplMethod())
	require.Equal(t, expectedSwitch, sfi.asSwitchCase())
}
