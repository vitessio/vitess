// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"reflect"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
)

// This is a V3 file. Do not intermix with V2.

// Plan is a pre-built execution plan, ready to execute a given request context.
type Plan interface {
	// Execute performs the operations represented by the execution plan,
	// given a particular Request. Queries to tablets are sent through the
	// Request's router.
	Execute(ctx context.Context, req *Request) (*sqltypes.Result, error)
	// GetFields fetches only the fields for a given Plan, without actually
	// executing the full request.
	GetFields(ctx context.Context, req *Request) (*sqltypes.Result, error)
}

// MakePlan returns a Plan that represents the Primitive tree rooted at the
// given node.
func MakePlan(root Primitive) (Plan, error) {
	// A Primitive tree is a valid Plan if all nodes recursively implement
	// the Plan interface, i.e. Execute().
	if root == nil {
		return nil, fmt.Errorf("can't make execution plan from empty primitive")
	}
	plan, ok := root.(Plan)
	if !ok {
		// root doesn't implement Plan. What's the actual struct type?
		t := reflect.ValueOf(root).Elem().Type()
		// This should produce something like: "GroupBy primitive is not implemented"
		return nil, fmt.Errorf("%s primitive is not implemented", t.Name())
	}
	// Validate children.
	for _, child := range root.Children() {
		if _, err := MakePlan(child); err != nil {
			// Add our own struct type to the error message to show which branch
			// of the tree failed.
			t := reflect.ValueOf(root).Elem().Type()
			// This should produce something like:
			//   "Select: CorrelatedFilter primitive is not implemented"
			return nil, fmt.Errorf("%s: %v", t.Name(), err)
		}
	}
	return plan, nil
}
