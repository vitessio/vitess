/*
Copyright 2025 The Vitess Authors.

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
// Code generated by ASTHelperGen. DO NOT EDIT.

package integration

// CloneAST creates a deep clone of the input.
func CloneAST(in AST) AST {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case BasicType:
		return in
	case Bytes:
		return CloneBytes(in)
	case InterfaceContainer:
		return CloneInterfaceContainer(in)
	case InterfaceSlice:
		return CloneInterfaceSlice(in)
	case *Leaf:
		return CloneRefOfLeaf(in)
	case LeafSlice:
		return CloneLeafSlice(in)
	case *NoCloneType:
		return CloneRefOfNoCloneType(in)
	case *RefContainer:
		return CloneRefOfRefContainer(in)
	case *RefSliceContainer:
		return CloneRefOfRefSliceContainer(in)
	case *SubImpl:
		return CloneRefOfSubImpl(in)
	case ValueContainer:
		return CloneValueContainer(in)
	case ValueSliceContainer:
		return CloneValueSliceContainer(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneBytes creates a deep clone of the input.
func CloneBytes(n Bytes) Bytes {
	if n == nil {
		return nil
	}
	res := make(Bytes, len(n))
	for i, x := range n {
		res[i] = x
	}
	return res
}

// CloneInterfaceContainer creates a deep clone of the input.
func CloneInterfaceContainer(n InterfaceContainer) InterfaceContainer {
	return *CloneRefOfInterfaceContainer(&n)
}

// CloneInterfaceSlice creates a deep clone of the input.
func CloneInterfaceSlice(n InterfaceSlice) InterfaceSlice {
	if n == nil {
		return nil
	}
	res := make(InterfaceSlice, len(n))
	for i, x := range n {
		res[i] = CloneAST(x)
	}
	return res
}

// CloneRefOfLeaf creates a deep clone of the input.
func CloneRefOfLeaf(n *Leaf) *Leaf {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneLeafSlice creates a deep clone of the input.
func CloneLeafSlice(n LeafSlice) LeafSlice {
	if n == nil {
		return nil
	}
	res := make(LeafSlice, len(n))
	for i, x := range n {
		res[i] = CloneRefOfLeaf(x)
	}
	return res
}

// CloneRefOfNoCloneType creates a deep clone of the input.
func CloneRefOfNoCloneType(n *NoCloneType) *NoCloneType {
	return n
}

// CloneRefOfRefContainer creates a deep clone of the input.
func CloneRefOfRefContainer(n *RefContainer) *RefContainer {
	if n == nil {
		return nil
	}
	out := *n
	out.ASTType = CloneAST(n.ASTType)
	out.Opts = CloneSliceOfRefOfOptions(n.Opts)
	out.ASTImplementationType = CloneRefOfLeaf(n.ASTImplementationType)
	return &out
}

// CloneRefOfRefSliceContainer creates a deep clone of the input.
func CloneRefOfRefSliceContainer(n *RefSliceContainer) *RefSliceContainer {
	if n == nil {
		return nil
	}
	out := *n
	out.ASTElements = CloneSliceOfAST(n.ASTElements)
	out.NotASTElements = CloneSliceOfInt(n.NotASTElements)
	out.ASTImplementationElements = CloneSliceOfRefOfLeaf(n.ASTImplementationElements)
	return &out
}

// CloneRefOfSubImpl creates a deep clone of the input.
func CloneRefOfSubImpl(n *SubImpl) *SubImpl {
	if n == nil {
		return nil
	}
	out := *n
	out.inner = CloneSubIface(n.inner)
	out.field = CloneRefOfBool(n.field)
	return &out
}

// CloneValueContainer creates a deep clone of the input.
func CloneValueContainer(n ValueContainer) ValueContainer {
	return *CloneRefOfValueContainer(&n)
}

// CloneValueSliceContainer creates a deep clone of the input.
func CloneValueSliceContainer(n ValueSliceContainer) ValueSliceContainer {
	return *CloneRefOfValueSliceContainer(&n)
}

// CloneSubIface creates a deep clone of the input.
func CloneSubIface(in SubIface) SubIface {
	if in == nil {
		return nil
	}
	switch in := in.(type) {
	case *SubImpl:
		return CloneRefOfSubImpl(in)
	default:
		// this should never happen
		return nil
	}
}

// CloneRefOfInterfaceContainer creates a deep clone of the input.
func CloneRefOfInterfaceContainer(n *InterfaceContainer) *InterfaceContainer {
	if n == nil {
		return nil
	}
	out := *n
	out.v = n.v
	return &out
}

// CloneSliceOfRefOfOptions creates a deep clone of the input.
func CloneSliceOfRefOfOptions(n []*Options) []*Options {
	if n == nil {
		return nil
	}
	res := make([]*Options, len(n))
	for i, x := range n {
		res[i] = CloneRefOfOptions(x)
	}
	return res
}

// CloneSliceOfAST creates a deep clone of the input.
func CloneSliceOfAST(n []AST) []AST {
	if n == nil {
		return nil
	}
	res := make([]AST, len(n))
	for i, x := range n {
		res[i] = CloneAST(x)
	}
	return res
}

// CloneSliceOfInt creates a deep clone of the input.
func CloneSliceOfInt(n []int) []int {
	if n == nil {
		return nil
	}
	res := make([]int, len(n))
	copy(res, n)
	return res
}

// CloneSliceOfRefOfLeaf creates a deep clone of the input.
func CloneSliceOfRefOfLeaf(n []*Leaf) []*Leaf {
	if n == nil {
		return nil
	}
	res := make([]*Leaf, len(n))
	for i, x := range n {
		res[i] = CloneRefOfLeaf(x)
	}
	return res
}

// CloneRefOfBool creates a deep clone of the input.
func CloneRefOfBool(n *bool) *bool {
	if n == nil {
		return nil
	}
	out := *n
	return &out
}

// CloneRefOfValueContainer creates a deep clone of the input.
func CloneRefOfValueContainer(n *ValueContainer) *ValueContainer {
	if n == nil {
		return nil
	}
	out := *n
	out.ASTType = CloneAST(n.ASTType)
	out.ASTImplementationType = CloneRefOfLeaf(n.ASTImplementationType)
	return &out
}

// CloneRefOfValueSliceContainer creates a deep clone of the input.
func CloneRefOfValueSliceContainer(n *ValueSliceContainer) *ValueSliceContainer {
	if n == nil {
		return nil
	}
	out := *n
	out.ASTElements = CloneSliceOfAST(n.ASTElements)
	out.NotASTElements = CloneSliceOfInt(n.NotASTElements)
	out.ASTImplementationElements = CloneLeafSlice(n.ASTImplementationElements)
	return &out
}

// CloneRefOfOptions creates a deep clone of the input.
func CloneRefOfOptions(n *Options) *Options {
	if n == nil {
		return nil
	}
	out := *n
	out.l = CloneRefOfLeaf(n.l)
	return &out
}
