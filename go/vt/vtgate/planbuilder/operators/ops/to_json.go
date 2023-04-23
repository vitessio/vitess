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
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/xlab/treeprint"
)

// ToJSON is a debug only function. It can panic, so do not use this in production code
func ToJSON(op Operator) string {
	descr := buildDescriptionTree(op)
	out, err := json.MarshalIndent(descr, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(out)
}

func buildDescriptionTree(op Operator) OpDescription {
	descr := op.Description()
	for _, in := range op.Inputs() {
		descr.Inputs = append(descr.Inputs, buildDescriptionTree(in))
	}
	return descr
}

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
