/*
Copyright 2021 The Vitess Authors.

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

package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCloneLeaf(t *testing.T) {
	leaf1 := &Leaf{1}
	clone := CloneRefOfLeaf(leaf1)
	assert.Equal(t, leaf1, clone)
	leaf1.v = 5
	assert.NotEqual(t, leaf1, clone)
}

func TestClone2(t *testing.T) {
	container := &RefContainer{
		ASTType:               &RefContainer{},
		NotASTType:            0,
		ASTImplementationType: &Leaf{2},
	}
	clone := CloneRefOfRefContainer(container)
	assert.Equal(t, container, clone)
	container.ASTImplementationType.v = 5
	assert.NotEqual(t, container, clone)
}

func TestTypeException(t *testing.T) {
	l1 := &Leaf{1}
	nc := &NoCloneType{1}

	slice := InterfaceSlice{
		l1,
		nc,
	}

	clone := CloneAST(slice)

	// change the original values
	l1.v = 99
	nc.v = 99

	expected := InterfaceSlice{
		&Leaf{1},         // the change is not seen
		&NoCloneType{99}, // since this type is not cloned, we do see the change
	}

	assert.Equal(t, expected, clone)
}
