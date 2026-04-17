/*
Copyright 2020 The Vitess Authors.

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

package sqlparser

import "slices"

// BindVarNeeds represents the bind vars that need to be provided as the result of expression rewriting.
type BindVarNeeds struct {
	NeedFunctionResult,
	NeedSystemVariable,
	// NeedUserDefinedVariables keeps track of all user defined variables a query is using
	NeedUserDefinedVariables []string
	otherRewrites int
}

// AddFuncResult adds a function bindvar need
func (bvn *BindVarNeeds) AddFuncResult(name string) {
	bvn.NeedFunctionResult = append(bvn.NeedFunctionResult, name)
}

// AddSysVar adds a system variable bindvar need
func (bvn *BindVarNeeds) AddSysVar(name string) {
	bvn.NeedSystemVariable = append(bvn.NeedSystemVariable, name)
}

// AddUserDefVar adds a user defined variable bindvar need
func (bvn *BindVarNeeds) AddUserDefVar(name string) {
	bvn.NeedUserDefinedVariables = append(bvn.NeedUserDefinedVariables, name)
}

// NeedsFuncResult says if a function result needs to be provided
func (bvn *BindVarNeeds) NeedsFuncResult(name string) bool {
	return contains(bvn.NeedFunctionResult, name)
}

// NeedsSysVar says if a function result needs to be provided
func (bvn *BindVarNeeds) NeedsSysVar(name string) bool {
	return contains(bvn.NeedSystemVariable, name)
}

func (bvn *BindVarNeeds) NoteRewrite() {
	bvn.otherRewrites++
}

func (bvn *BindVarNeeds) NumberOfRewrites() int {
	return len(bvn.NeedFunctionResult) + len(bvn.NeedUserDefinedVariables) + len(bvn.NeedSystemVariable) + bvn.otherRewrites
}

func contains(strings []string, name string) bool {
	return slices.Contains(strings, name)
}
