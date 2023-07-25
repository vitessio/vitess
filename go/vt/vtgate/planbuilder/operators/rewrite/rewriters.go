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
	"fmt"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	// VisitF is the visitor that walks an operator tree
	VisitF func(
		op ops.Operator, // op is the operator being visited
		lhsTables semantics.TableSet, // lhsTables contains the TableSet for all table on the LHS of our parent
		isRoot bool, // isRoot will be true for the root of the operator tree
	) (ops.Operator, *ApplyResult, error)

	// ShouldVisit is used when we want to control which nodes and ancestors to visit and which to skip
	ShouldVisit func(ops.Operator) VisitRule

	// ApplyResult tracks modifications to node and expression trees.
	// Only return SameTree when it is acceptable to return the original
	// input and discard the returned result as a performance improvement.
	ApplyResult struct {
		Transformations []Rewrite
	}

	Rewrite struct {
		Message string
		Op      ops.Operator
	}

	// VisitRule signals to the rewriter if the children of this operator should be visited or not
	VisitRule bool
)

var (
	SameTree *ApplyResult = nil
)

const (
	VisitChildren VisitRule = true
	SkipChildren  VisitRule = false
)

func NewTree(message string, op ops.Operator) *ApplyResult {
	if DebugOperatorTree {
		fmt.Println(">>>>>>>> " + message)
	}
	return &ApplyResult{Transformations: []Rewrite{{Message: message, Op: op}}}
}

func (ar *ApplyResult) Merge(other *ApplyResult) *ApplyResult {
	if ar == nil {
		return other
	}
	if other == nil {
		return ar
	}
	return &ApplyResult{Transformations: append(ar.Transformations, other.Transformations...)}
}

func (ar *ApplyResult) Changed() bool {
	return ar != nil
}

// Visit allows for the walking of the operator tree. If any error is returned, the walk is aborted
func Visit(root ops.Operator, visitor func(ops.Operator) error) error {
	_, _, err := breakableTopDown(root, func(op ops.Operator) (ops.Operator, *ApplyResult, VisitRule, error) {
		err := visitor(op)
		if err != nil {
			return nil, SameTree, SkipChildren, err
		}
		return op, SameTree, VisitChildren, nil
	})
	return err
}

// BottomUp rewrites an operator tree from the bottom up. BottomUp applies a transformation function to
// the given operator tree from the bottom up. Each callback [f] returns a ApplyResult that is aggregated
// into a final output indicating whether the operator tree was changed.
func BottomUp(
	root ops.Operator,
	resolveID func(ops.Operator) semantics.TableSet,
	visit VisitF,
	shouldVisit ShouldVisit,
) (ops.Operator, error) {
	op, _, err := bottomUp(root, semantics.EmptyTableSet(), resolveID, visit, shouldVisit, true)
	if err != nil {
		return nil, err
	}
	return op, nil
}

var DebugOperatorTree = false

func EnableDebugPrinting() (reset func()) {
	t := DebugOperatorTree
	DebugOperatorTree = true
	return func() {
		DebugOperatorTree = t
	}
}

// FixedPointBottomUp rewrites an operator tree much like BottomUp does,
// but does the rewriting repeatedly, until a tree walk is done with no changes to the tree.
func FixedPointBottomUp(
	root ops.Operator,
	resolveID func(ops.Operator) semantics.TableSet,
	visit VisitF,
	shouldVisit ShouldVisit,
) (op ops.Operator, err error) {
	var id *ApplyResult
	op = root
	// will loop while the rewriting changes anything
	for ok := true; ok; ok = id != SameTree {
		if DebugOperatorTree {
			fmt.Println(ops.ToTree(op))
		}
		// Continue the top-down rewriting process as long as changes were made during the last traversal
		op, id, err = bottomUp(op, semantics.EmptyTableSet(), resolveID, visit, shouldVisit, true)
		if err != nil {
			return nil, err
		}
	}

	return op, nil
}

// BottomUpAll rewrites an operator tree from the bottom up. BottomUp applies a transformation function to
// the given operator tree from the bottom up. Each callback [f] returns a ApplyResult that is aggregated
// into a final output indicating whether the operator tree was changed.
func BottomUpAll(
	root ops.Operator,
	resolveID func(ops.Operator) semantics.TableSet,
	visit VisitF,
) (ops.Operator, error) {
	return BottomUp(root, resolveID, visit, func(ops.Operator) VisitRule {
		return VisitChildren
	})
}

// TopDown rewrites an operator tree from the bottom up. BottomUp applies a transformation function to
// the given operator tree from the bottom up. Each callback [f] returns a ApplyResult that is aggregated
// into a final output indicating whether the operator tree was changed.
//
// Parameters:
// - root: The root operator of the tree to be traversed.
// - resolveID: A function to resolve the TableSet of an operator.
// - visit: The VisitF function to be called for each visited operator.
// - shouldVisit: The ShouldVisit function to control which nodes and ancestors to visit and which to skip.
//
// Returns:
// - ops.Operator: The root of the (potentially) transformed operator tree.
// - error: An error if any occurred during the traversal.
func TopDown(
	root ops.Operator,
	resolveID func(ops.Operator) semantics.TableSet,
	visit VisitF,
	shouldVisit ShouldVisit,
) (op ops.Operator, err error) {
	op, _, err = topDown(root, semantics.EmptyTableSet(), resolveID, visit, shouldVisit, true)
	if err != nil {
		return nil, err
	}

	return op, nil
}

// Swap takes a tree like a->b->c and swaps `a` and `b`, so we end up with b->a->c
func Swap(parent, child ops.Operator, message string) (ops.Operator, *ApplyResult, error) {
	c := child.Inputs()
	if len(c) != 1 {
		return nil, nil, vterrors.VT13001("Swap can only be used on single input operators")
	}

	aInputs := slices.Clone(parent.Inputs())
	var tmp ops.Operator
	for i, in := range aInputs {
		if in == child {
			tmp = aInputs[i]
			aInputs[i] = c[0]
			break
		}
	}
	if tmp == nil {
		return nil, nil, vterrors.VT13001("Swap can only be used when the second argument is an input to the first")
	}

	child.SetInputs([]ops.Operator{parent})
	parent.SetInputs(aInputs)

	return child, NewTree(message, parent), nil
}

func bottomUp(
	root ops.Operator,
	rootID semantics.TableSet,
	resolveID func(ops.Operator) semantics.TableSet,
	rewriter VisitF,
	shouldVisit ShouldVisit,
	isRoot bool,
) (ops.Operator, *ApplyResult, error) {
	if !shouldVisit(root) {
		return root, SameTree, nil
	}

	oldInputs := root.Inputs()
	var anythingChanged *ApplyResult
	newInputs := make([]ops.Operator, len(oldInputs))
	childID := rootID

	// noLHSTableSet is used to mark which operators that do not send data from the LHS to the RHS
	// It's only UNION at this moment, but this package can't depend on the actual operators, so
	// we use this interface to avoid direct dependencies
	type noLHSTableSet interface{ NoLHSTableSet() }

	for i, operator := range oldInputs {
		// We merge the table set of all the LHS above the current root so that we can
		// send it down to the current RHS.
		// We don't want to send the LHS table set to the RHS if the root is a UNION.
		// Some operators, like SubQuery, can have multiple child operators on the RHS
		if _, isUnion := root.(noLHSTableSet); !isUnion && i > 0 {
			childID = childID.Merge(resolveID(oldInputs[0]))
		}
		in, changed, err := bottomUp(operator, childID, resolveID, rewriter, shouldVisit, false)
		if err != nil {
			return nil, nil, err
		}
		anythingChanged = anythingChanged.Merge(changed)
		newInputs[i] = in
	}

	if anythingChanged.Changed() {
		root = root.Clone(newInputs)
	}

	newOp, treeIdentity, err := rewriter(root, rootID, isRoot)
	if err != nil {
		return nil, nil, err
	}
	anythingChanged = anythingChanged.Merge(treeIdentity)
	return newOp, anythingChanged, nil
}

func breakableTopDown(
	in ops.Operator,
	rewriter func(ops.Operator) (ops.Operator, *ApplyResult, VisitRule, error),
) (ops.Operator, *ApplyResult, error) {
	newOp, identity, visit, err := rewriter(in)
	if err != nil || visit == SkipChildren {
		return newOp, identity, err
	}

	var anythingChanged *ApplyResult

	oldInputs := newOp.Inputs()
	newInputs := make([]ops.Operator, len(oldInputs))
	for i, oldInput := range oldInputs {
		newInputs[i], identity, err = breakableTopDown(oldInput, rewriter)
		anythingChanged = anythingChanged.Merge(identity)
		if err != nil {
			return nil, SameTree, err
		}
	}

	if anythingChanged.Changed() {
		return newOp, SameTree, nil
	}

	return newOp.Clone(newInputs), anythingChanged, nil
}

// topDown is a helper function that recursively traverses the operator tree from the
// top down and applies the given transformation function. It also returns the ApplyResult
// indicating whether the tree was changed
func topDown(
	root ops.Operator,
	rootID semantics.TableSet,
	resolveID func(ops.Operator) semantics.TableSet,
	rewriter VisitF,
	shouldVisit ShouldVisit,
	isRoot bool,
) (ops.Operator, *ApplyResult, error) {
	newOp, anythingChanged, err := rewriter(root, rootID, isRoot)
	if err != nil {
		return nil, nil, err
	}

	if !shouldVisit(root) {
		return newOp, anythingChanged, nil
	}

	if anythingChanged.Changed() {
		root = newOp
	}

	oldInputs := root.Inputs()
	newInputs := make([]ops.Operator, len(oldInputs))
	childID := rootID

	type noLHSTableSet interface{ NoLHSTableSet() }

	for i, operator := range oldInputs {
		if _, isUnion := root.(noLHSTableSet); !isUnion && i > 0 {
			childID = childID.Merge(resolveID(oldInputs[0]))
		}
		in, changed, err := topDown(operator, childID, resolveID, rewriter, shouldVisit, false)
		if err != nil {
			return nil, nil, err
		}
		anythingChanged = anythingChanged.Merge(changed)
		newInputs[i] = in
	}

	if anythingChanged != SameTree {
		return root.Clone(newInputs), anythingChanged, nil
	}

	return root, SameTree, nil
}
