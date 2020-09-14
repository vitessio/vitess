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

// BindVarNeeds represents the bind vars that need to be provided as the result of expression rewriting.
type BindVarNeeds struct {
	needFunctionResult,
	needSystemVariable,
	// NeedUserDefinedVariables keeps track of all user defined variables a query is using
	NeedUserDefinedVariables []string
}

//MergeWith adds bind vars needs coming from sub scopes
func (bvn *BindVarNeeds) MergeWith(other *BindVarNeeds) {
	bvn.needFunctionResult = append(bvn.needFunctionResult, other.needFunctionResult...)
	bvn.needSystemVariable = append(bvn.needSystemVariable, other.needSystemVariable...)
	bvn.NeedUserDefinedVariables = append(bvn.NeedUserDefinedVariables, other.NeedUserDefinedVariables...)
}

//AddFuncResult adds a function bindvar need
func (bvn *BindVarNeeds) AddFuncResult(name string) {
	bvn.needFunctionResult = append(bvn.needFunctionResult, name)
}

//AddSysVar adds a system variable bindvar need
func (bvn *BindVarNeeds) AddSysVar(name string) {
	bvn.needSystemVariable = append(bvn.needSystemVariable, name)
}

//AddUserDefVar adds a user defined variable bindvar need
func (bvn *BindVarNeeds) AddUserDefVar(name string) {
	bvn.NeedUserDefinedVariables = append(bvn.NeedUserDefinedVariables, name)
}

//NeedsFuncResult says if a function result needs to be provided
func (bvn *BindVarNeeds) NeedsFuncResult(name string) bool {
	return contains(bvn.needFunctionResult, name)
}

//NeedsSysVar says if a function result needs to be provided
func (bvn *BindVarNeeds) NeedsSysVar(name string) bool {
	return contains(bvn.needSystemVariable, name)
}

func contains(strings []string, name string) bool {
	for _, s := range strings {
		if name == s {
			return true
		}
	}
	return false
}
