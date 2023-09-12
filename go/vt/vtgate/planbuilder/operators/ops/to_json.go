/*
Copyright 2023 The Vitess Authors.

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

package ops

import (
	"fmt"
	"reflect"

	"github.com/xlab/treeprint"
)

// ToTree returns the operator as ascii tree. Should only be used for debugging
func ToTree(op Operator) string {
	tree := asTree(op, nil)
	return tree.String()
}

func opDescr(op Operator) string {
	typ := reflect.TypeOf(op).Elem().Name()
	shortDescription := op.ShortDescription
	if shortDescription() == "" {
		return typ
	}
	return fmt.Sprintf("%s (%s)", typ, shortDescription())
}

func asTree(op Operator, root treeprint.Tree) treeprint.Tree {
	txt := opDescr(op)
	var branch treeprint.Tree
	if root == nil {
		branch = treeprint.NewWithRoot(txt)
	} else {
		branch = root.AddBranch(txt)
	}
	for _, child := range op.Inputs() {
		asTree(child, branch)
	}
	return branch
}
