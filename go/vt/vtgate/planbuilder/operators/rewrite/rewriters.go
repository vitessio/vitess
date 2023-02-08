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
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	Func          func(semantics.TableSet, ops.Operator) (ops.Operator, TreeIdentity, error)
	BreakableFunc func(ops.Operator) (ops.Operator, TreeIdentity, VisitRule, error)

	// TreeIdentity tracks modifications to node and expression trees.
	// Only return SameTree when it is acceptable to return the original
	// input and discard the returned result as a performance improvement.
	TreeIdentity bool

	// VisitRule signals to the rewriter if the children of this operator should be visited or not
	VisitRule bool
)

const (
	SameTree TreeIdentity = false
	NewTree  TreeIdentity = true

	VisitChildren VisitRule = true
	SkipChildren  VisitRule = false
)

// Visit allows for the walking of the operator tree. If any error is returned, the walk is aborted
func Visit(root ops.Operator, visitor func(ops.Operator) error) error {
	_, err := TopDown(root, func(op ops.Operator) (ops.Operator, TreeIdentity, VisitRule, error) {
		err := visitor(op)
		if err != nil {
			return nil, SameTree, SkipChildren, err
		}
		return op, SameTree, VisitChildren, nil
	})
	return err
}

// BottomUp rewrites an operator tree from the bottom up. BottomUp applies a transformation function to
// the given operator tree from the bottom up. Each callback [f] returns a TreeIdentity that is aggregated
// into a final output indicating whether the operator tree was changed.
func BottomUp(root ops.Operator, rootID semantics.TableSet, resolveID func(ops.Operator) semantics.TableSet, f Func) (ops.Operator, error) {
	op, _, err := bottomUp(root, rootID, resolveID, f)
	if err != nil {
		return nil, err
	}
	return op, nil
}

// TopDown applies a transformation function to the given operator tree from the bottom up. =
// Each callback [f] returns a TreeIdentity that is aggregated into a final output indicating whether the
// operator tree was changed.
// The callback also returns a VisitRule that signals whether the children of this operator should be visited or not
func TopDown(in ops.Operator, rewriter BreakableFunc) (ops.Operator, error) {
	op, _, err := breakableTopDown(in, rewriter)
	return op, err
}

func bottomUp(root ops.Operator, rootID semantics.TableSet, resolveID func(ops.Operator) semantics.TableSet, rewriter Func) (ops.Operator, TreeIdentity, error) {
	oldInputs := root.Inputs()
	anythingChanged := false
	newInputs := make([]ops.Operator, len(oldInputs))
	childID := rootID

	// noLHSTableSet is used to mark which operators that do not send data from the LHS to the RHS
	// It's only UNION at this moment, but this package can't depend on the actual operators, so
	// we use this interface to avoid direct dependencies
	type noLHSTableSet interface{ NoLHSTableSet() }

	for i, operator := range oldInputs {
		// We merge the table set of all the LHS above the current root so that we can
		// send it down to the current RHS.
		// We don't want to send the LHS table set to the RHS if the root is an UNION.
		// Some operators, like SubQuery, can have multiple child operators on the RHS
		if _, isUnion := root.(noLHSTableSet); !isUnion && i > 0 {
			childID = childID.Merge(resolveID(oldInputs[0]))
		}
		in, changed, err := bottomUp(operator, childID, resolveID, rewriter)
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

	newOp, treeIdentity, err := rewriter(rootID, root)
	if err != nil {
		return nil, SameTree, err
	}
	if anythingChanged {
		treeIdentity = NewTree
	}
	return newOp, treeIdentity, nil
}

func breakableTopDown(in ops.Operator, rewriter BreakableFunc) (ops.Operator, TreeIdentity, error) {
	newOp, identity, visit, err := rewriter(in)
	if err != nil || visit == SkipChildren {
		return newOp, identity, err
	}

	anythingChanged := identity == NewTree

	oldInputs := newOp.Inputs()
	newInputs := make([]ops.Operator, len(oldInputs))
	for i, oldInput := range oldInputs {
		newInputs[i], identity, err = breakableTopDown(oldInput, rewriter)
		anythingChanged = anythingChanged || identity == NewTree
		if err != nil {
			return nil, SameTree, err
		}
	}

	if anythingChanged {
		return newOp.Clone(newInputs), NewTree, nil
	}

	return newOp, SameTree, nil
}
