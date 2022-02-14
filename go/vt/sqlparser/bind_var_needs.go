package sqlparser

// BindVarNeeds represents the bind vars that need to be provided as the result of expression rewriting.
type BindVarNeeds struct {
	NeedFunctionResult,
	NeedSystemVariable,
	// NeedUserDefinedVariables keeps track of all user defined variables a query is using
	NeedUserDefinedVariables []string
	otherRewrites bool
}

//MergeWith adds bind vars needs coming from sub scopes
func (bvn *BindVarNeeds) MergeWith(other *BindVarNeeds) {
	bvn.NeedFunctionResult = append(bvn.NeedFunctionResult, other.NeedFunctionResult...)
	bvn.NeedSystemVariable = append(bvn.NeedSystemVariable, other.NeedSystemVariable...)
	bvn.NeedUserDefinedVariables = append(bvn.NeedUserDefinedVariables, other.NeedUserDefinedVariables...)
}

//AddFuncResult adds a function bindvar need
func (bvn *BindVarNeeds) AddFuncResult(name string) {
	bvn.NeedFunctionResult = append(bvn.NeedFunctionResult, name)
}

//AddSysVar adds a system variable bindvar need
func (bvn *BindVarNeeds) AddSysVar(name string) {
	bvn.NeedSystemVariable = append(bvn.NeedSystemVariable, name)
}

//AddUserDefVar adds a user defined variable bindvar need
func (bvn *BindVarNeeds) AddUserDefVar(name string) {
	bvn.NeedUserDefinedVariables = append(bvn.NeedUserDefinedVariables, name)
}

//NeedsFuncResult says if a function result needs to be provided
func (bvn *BindVarNeeds) NeedsFuncResult(name string) bool {
	return contains(bvn.NeedFunctionResult, name)
}

//NeedsSysVar says if a function result needs to be provided
func (bvn *BindVarNeeds) NeedsSysVar(name string) bool {
	return contains(bvn.NeedSystemVariable, name)
}

func (bvn *BindVarNeeds) NoteRewrite() {
	bvn.otherRewrites = true
}

func (bvn *BindVarNeeds) HasRewrites() bool {
	return bvn.otherRewrites ||
		len(bvn.NeedFunctionResult) > 0 ||
		len(bvn.NeedUserDefinedVariables) > 0 ||
		len(bvn.NeedSystemVariable) > 0
}

func contains(strings []string, name string) bool {
	for _, s := range strings {
		if name == s {
			return true
		}
	}
	return false
}
