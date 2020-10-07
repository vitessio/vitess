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

import (
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// RewriteASTResult contains the rewritten ast and meta information about it
type RewriteASTResult struct {
	*BindVarNeeds
	AST Statement // The rewritten AST
}

// PrepareAST will normalize the query
func PrepareAST(in Statement, bindVars map[string]*querypb.BindVariable, prefix string, parameterize bool) (*RewriteASTResult, error) {
	if parameterize {
		Normalize(in, bindVars, prefix)
	}
	return RewriteAST(in)
}

// RewriteAST rewrites the whole AST, replacing function calls and adding column aliases to queries
func RewriteAST(in Statement) (*RewriteASTResult, error) {
	er := newExpressionRewriter()
	er.shouldRewriteDatabaseFunc = shouldRewriteDatabaseFunc(in)
	setRewriter := &setNormalizer{}
	out, ok := Rewrite(in, er.rewrite, setRewriter.rewriteSetComingUp).(Statement)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "statement rewriting returned a non statement: %s", String(out))
	}
	if setRewriter.err != nil {
		return nil, setRewriter.err
	}

	r := &RewriteASTResult{
		AST:          out,
		BindVarNeeds: er.bindVars,
	}
	return r, nil
}
