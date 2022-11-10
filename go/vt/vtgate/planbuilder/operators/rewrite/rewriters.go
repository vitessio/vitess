/*
Copyright 2022 The Vitess Authors.

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

package rewrite

import (
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	Func          func(*plancontext.PlanningContext, ops.Operator) (ops.Operator, TreeIdentity, error)
	BreakableFunc func(*plancontext.PlanningContext, ops.Operator) (ops.Operator, VisitRule, error)

	// TreeIdentity tracks modifications to node and expression trees.
	// Only return SameTree when it is acceptable to return the original
	// input and discard the returned result as a performance improvement.
	TreeIdentity bool
	VisitRule    bool
)

const (
	SameTree TreeIdentity = false
	NewTree  TreeIdentity = true

	VisitChildren VisitRule = true
	SkipChildren  VisitRule = false
)

func VisitTopDown(root ops.Operator, visitor func(ops.Operator) error) error {
	queue := []ops.Operator{root}
	for len(queue) > 0 {
		this := queue[0]
		queue = append(queue[1:], this.Inputs()...)
		err := visitor(this)
		if err != nil {
			return err
		}
	}
	return nil
}

func BottomUp(ctx *plancontext.PlanningContext, root ops.Operator, rewriter Func) (ops.Operator, error) {
	op, _, err := bottomUp(ctx, root, rewriter)
	if err != nil {
		return nil, err
	}
	return op, nil
}

func bottomUp(ctx *plancontext.PlanningContext, root ops.Operator, rewriter Func) (ops.Operator, TreeIdentity, error) {
	oldInputs := root.Inputs()
	anythingChanged := false
	newInputs := make([]ops.Operator, len(oldInputs))
	for i, operator := range oldInputs {
		in, changed, err := bottomUp(ctx, operator, rewriter)
		if err != nil {
			return nil, SameTree, err
		}
		if changed == NewTree {
			anythingChanged = true
		}
		newInputs[i] = in
	}

	if anythingChanged {
		root = root.Clone(newInputs)
	}

	newOp, treeIdentity, err := rewriter(ctx, root)
	if err != nil {
		return nil, SameTree, err
	}
	if anythingChanged {
		treeIdentity = NewTree
	}
	return newOp, treeIdentity, nil
}

func BreakableTopDown(ctx *plancontext.PlanningContext, in ops.Operator, rewriter BreakableFunc) (ops.Operator, error) {
	newOp, visitChildren, err := rewriter(ctx, in)
	if err != nil || visitChildren == VisitChildren {
		return newOp, err
	}

	oldInputs := newOp.Inputs()
	newInputs := make([]ops.Operator, len(oldInputs))
	for i, oldInput := range oldInputs {
		newInputs[i], err = BreakableTopDown(ctx, oldInput, rewriter)
		if err != nil {
			return nil, err
		}
	}

	return newOp.Clone(newInputs), nil
}
