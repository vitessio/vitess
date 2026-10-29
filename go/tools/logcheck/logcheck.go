/*
Copyright 2026 The Vitess Authors.

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

// Package logcheck defines an analyzer that enforces Vitess's structured
// logging convention: log messages are constants, and values go in slog
// attributes.
//
// Messages built with fmt.Sprintf can carry row values, last primary keys,
// binlog events and query text, and redacting the rendered string is
// brittle. In the packages this analyzer checks, that data is only in
// attributes, so a log pipeline can keep msg and drop the attributes it
// doesn't want. To keep database data out of logs, keep only keys that carry
// no data, such as msg, level, time, table, column, workflow, stream_id,
// keyspace, shard, tablet and error_code.
//
// Keys that can hold data include:
//
//   - error
//   - query, filter and expression
//   - last_pk, row, values, event, transaction, message and panic
package logcheck

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/edge"
	"golang.org/x/tools/go/ast/inspector"
)

const (
	logPkgPath     = "vitess.io/vitess/go/vt/log"
	logutilPkgPath = "vitess.io/vitess/go/vt/logutil"
)

const doc = `check that structured log messages are constants

Functions, methods, func variables and func-typed fields in go/vt/log and
go/vt/logutil whose parameters end in (msg string, attrs ...slog.Attr) are
logging functions.

  - The message passed to a logging function must be a compile-time constant.
    Put values in slog attributes instead:

        log.Info("stream started", slog.String("table", name))

  - The prefix passed to log.NewPrefixedLogger must be a constant, because it
    is prepended to every message.

  - Logging functions and log.NewPrefixedLogger must only be called directly,
    not stored, passed, assigned or compared, so that every call is checked.

  - logutil.ThrottledLogger's Infof, Warningf and Errorf must not be called
    or referenced, because they format values into the message.

Test files and generated files are not checked. That includes files that use
cgo, since go vet is given the code cmd/cgo generates from them.`

// Analyzer checks every package it is run on; the caller chooses which
// packages those are.
var Analyzer = &analysis.Analyzer{
	Name:     "logcheck",
	Doc:      doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	// These packages assign and wrap their own logging functions.
	if trackedPath(pass.Pkg.Path()) {
		return nil, nil
	}

	ins := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	ins.Root().Inspect([]ast.Node{(*ast.File)(nil), (*ast.Ident)(nil)}, func(c inspector.Cursor) bool {
		switch n := c.Node().(type) {
		case *ast.File:
			return !skipFile(pass, n)
		case *ast.Ident:
			checkIdent(pass, c, n)
		}
		return false
	})
	return nil, nil
}

func skipFile(pass *analysis.Pass, f *ast.File) bool {
	return strings.HasSuffix(pass.Fset.File(f.FileStart).Name(), "_test.go") || ast.IsGenerated(f)
}

// checkIdent is called for every identifier, so the common path is one
// Uses lookup and a package path comparison.
func checkIdent(pass *analysis.Pass, c inspector.Cursor, id *ast.Ident) {
	obj := pass.TypesInfo.Uses[id]
	if obj == nil || obj.Pkg() == nil || !trackedPath(obj.Pkg().Path()) {
		return
	}

	if isThrottledPrintf(obj) {
		pass.ReportRangef(useNode(c), "ThrottledLogger.%s formats values into the message; log a constant message with slog attributes", obj.Name())
		return
	}

	isPrefix := isNewPrefixedLogger(obj)
	argIdx := 0
	if !isPrefix {
		argIdx = messageParam(obj)
		if argIdx < 0 {
			return
		}
	}

	call, sel := directCall(c)
	if call == nil {
		if kind, _ := c.ParentEdge(); kind == edge.KeyValueExpr_Key {
			// A field name in a composite literal names the field; the value
			// assigned to it is not a logging function.
			return
		}
		pass.ReportRangef(useNode(c), "logging function must be called directly, not used as a value")
		return
	}

	// A method expression, (*T).M(recv, ...), takes the receiver as its
	// first argument.
	if sel != nil {
		if selection := pass.TypesInfo.Selections[sel]; selection != nil && selection.Kind() == types.MethodExpr {
			argIdx++
		}
	}

	var arg ast.Expr
	switch {
	case argIdx < len(call.Args):
		arg = call.Args[argIdx]
	case len(call.Args) == 1:
		// f(g()), where g returns all of f's arguments.
		arg = call.Args[0]
	default:
		return
	}
	if pass.TypesInfo.Types[arg].Value != nil {
		return
	}
	if isPrefix {
		pass.ReportRangef(arg, "log prefix must be a constant; pass values as slog attributes")
	} else {
		pass.ReportRangef(arg, "log message must be a constant; pass values as slog attributes")
	}
}

// useNode returns the identifier at c, or its qualifying selector (pkg.F,
// x.M) if there is one, so that a finding covers the whole name.
func useNode(c inspector.Cursor) ast.Node {
	if kind, _ := c.ParentEdge(); kind == edge.SelectorExpr_Sel {
		return c.Parent().Node()
	}
	return c.Node()
}

func trackedPath(path string) bool {
	return path == logPkgPath || path == logutilPkgPath
}

func isNewPrefixedLogger(obj types.Object) bool {
	return obj.Name() == "NewPrefixedLogger" && obj.Pkg().Path() == logPkgPath
}

func isThrottledPrintf(obj types.Object) bool {
	fn, ok := obj.(*types.Func)
	if !ok || obj.Pkg().Path() != logutilPkgPath {
		return false
	}
	switch fn.Name() {
	case "Infof", "Warningf", "Errorf":
	default:
		return false
	}
	recv := fn.Signature().Recv()
	if recv == nil {
		return false
	}
	t := recv.Type()
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}
	named, ok := types.Unalias(t).(*types.Named)
	return ok && named.Obj().Name() == "ThrottledLogger"
}

// messageParam returns the index of the message parameter if obj is a
// function, method, func variable or func-typed field whose parameters end in
// (msg string, attrs ...slog.Attr), and -1 otherwise.
func messageParam(obj types.Object) int {
	var sig *types.Signature
	switch obj := obj.(type) {
	case *types.Func:
		sig = obj.Signature()
	case *types.Var:
		sig, _ = obj.Type().Underlying().(*types.Signature)
	}
	if sig == nil || !sig.Variadic() {
		return -1
	}
	params := sig.Params()
	n := params.Len()
	if n < 2 || !isString(params.At(n-2).Type()) || !isSlogAttrSlice(params.At(n-1).Type()) {
		return -1
	}
	return n - 2
}

func isString(t types.Type) bool {
	b, ok := t.(*types.Basic)
	return ok && b.Kind() == types.String
}

func isSlogAttrSlice(t types.Type) bool {
	slice, ok := t.(*types.Slice)
	if !ok {
		return false
	}
	named, ok := types.Unalias(slice.Elem()).(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj.Name() == "Attr" && obj.Pkg().Path() == "log/slog"
}

// directCall returns the call whose function is the identifier at c, allowing
// a qualifying selector (pkg.F, x.M, T.M) and parentheses around it, and the
// selector if there is one. It returns nil if the identifier is used any
// other way.
func directCall(c inspector.Cursor) (*ast.CallExpr, *ast.SelectorExpr) {
	var sel *ast.SelectorExpr
	for {
		kind, _ := c.ParentEdge()
		c = c.Parent()
		switch kind {
		case edge.SelectorExpr_Sel:
			sel = c.Node().(*ast.SelectorExpr)
		case edge.ParenExpr_X:
		case edge.CallExpr_Fun:
			return c.Node().(*ast.CallExpr), sel
		default:
			return nil, nil
		}
	}
}
