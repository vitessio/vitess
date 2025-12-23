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

package main

import (
	"go/ast"
	"go/parser"
	gotoken "go/token"
	"go/types"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/packages"
)

// Helper function to create a rewriter
func createTestRewriter() *Rewriter {
	exprInterface := types.NewInterfaceType(nil, nil)

	return &Rewriter{
		pkg: &packages.Package{
			TypesInfo: &types.Info{
				Types: make(map[ast.Expr]types.TypeAndValue),
			},
		},
		astExpr: exprInterface,
	}
}

// Helper function to create a call expression with proper AST structure
func createCallExpr(receiver, method string, args []ast.Expr) *ast.CallExpr {
	return &ast.CallExpr{
		Fun: &ast.SelectorExpr{
			X:   &ast.Ident{Name: receiver},
			Sel: &ast.Ident{Name: method},
		},
		Args: args,
	}
}

// Helper to wrap statement in a function for astutil.Apply
func wrapInFunc(stmt ast.Stmt) *ast.File {
	return &ast.File{
		Decls: []ast.Decl{
			&ast.FuncDecl{
				Name: &ast.Ident{Name: "test"},
				Type: &ast.FuncType{},
				Body: &ast.BlockStmt{List: []ast.Stmt{stmt}},
			},
		},
	}
}

func TestMethodName(t *testing.T) {
	tests := []struct {
		name     string
		code     string
		expected string
	}{
		{
			name:     "selector expression with method call",
			code:     `package test; import "fmt"; func test() { fmt.Println("test") }`,
			expected: "Println",
		},
		{
			name:     "simple function call without selector",
			code:     `package test; func foo() {}; func test() { foo() }`,
			expected: "",
		},
		{
			name:     "method call on variable",
			code:     `package test; type T struct{}; func (T) Method() {}; func test(t T) { t.Method() }`,
			expected: "Method",
		},
		{
			name:     "nil selector",
			code:     `package test; func test() { foo() }`,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fset := gotoken.NewFileSet()
			file, err := parser.ParseFile(fset, "test.go", tt.code, 0)
			require.NoError(t, err)

			r := createTestRewriter()

			// Find the first call expression in the AST
			var callExpr *ast.CallExpr
			ast.Inspect(file, func(n ast.Node) bool {
				if ce, ok := n.(*ast.CallExpr); ok && callExpr == nil {
					callExpr = ce
					return false
				}
				return true
			})

			if callExpr != nil {
				result := r.methodName(callExpr)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestRewriteLiteral(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		argValue       string
		argKind        gotoken.Token
		expectedMethod string
		verifyArg      func(*testing.T, ast.Expr)
	}{
		{
			name:           "WriteString method",
			method:         "WriteString",
			argValue:       `"hello"`,
			argKind:        gotoken.STRING,
			expectedMethod: "WriteString",
			verifyArg: func(t *testing.T, arg ast.Expr) {
				lit, ok := arg.(*ast.BasicLit)
				require.True(t, ok)
				assert.Equal(t, gotoken.STRING, lit.Kind)
				assert.Equal(t, `"hello"`, lit.Value)
			},
		},
		{
			name:           "WriteByte method",
			method:         "WriteByte",
			argValue:       "'x'",
			argKind:        gotoken.CHAR,
			expectedMethod: "WriteByte",
			verifyArg: func(t *testing.T, arg ast.Expr) {
				lit, ok := arg.(*ast.BasicLit)
				require.True(t, ok)
				assert.Equal(t, gotoken.CHAR, lit.Kind)
				assert.Equal(t, "'x'", lit.Value)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := createTestRewriter()

			rcv := &ast.Ident{Name: "buf"}
			arg := &ast.BasicLit{
				Kind:  tt.argKind,
				Value: tt.argValue,
			}

			stmt := r.rewriteLiteral(rcv, tt.method, arg)

			require.IsType(t, &ast.ExprStmt{}, stmt)
			exprStmt := stmt.(*ast.ExprStmt)

			require.IsType(t, &ast.CallExpr{}, exprStmt.X)
			callExpr := exprStmt.X.(*ast.CallExpr)

			require.IsType(t, &ast.SelectorExpr{}, callExpr.Fun)
			selExpr := callExpr.Fun.(*ast.SelectorExpr)

			assert.Equal(t, tt.expectedMethod, selExpr.Sel.Name)
			assert.Equal(t, rcv, selExpr.X)
			require.Len(t, callExpr.Args, 1)

			if tt.verifyArg != nil {
				tt.verifyArg(t, callExpr.Args[0])
			}
		})
	}
}

func TestReplaceAstfmtCalls_Comment(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "replace Format with FormatFast in doc comment with spaces",
			input:    "// This is a Format function",
			expected: "// This is a FormatFast function",
		},
		{
			name:     "multiple Format occurrences with spaces",
			input:    "// Format and Format again",
			expected: "// FormatFast and FormatFast again",
		},
		{
			name:     "no Format in comment",
			input:    "// This is a test",
			expected: "// This is a test",
		},
		{
			name:     "Format at start with trailing space",
			input:    "// Format is a method",
			expected: "// FormatFast is a method",
		},
		{
			name:     "Format at end with leading space",
			input:    "// This is Format ",
			expected: "// This is FormatFast ",
		},
		{
			name:     "Format in middle of word not replaced",
			input:    "// ReFormat or Format() method",
			expected: "// ReFormat or Format() method",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := createTestRewriter()

			comment := &ast.Comment{Text: tt.input}
			commentGroup := &ast.CommentGroup{List: []*ast.Comment{comment}}
			funcDecl := &ast.FuncDecl{
				Doc:  commentGroup,
				Name: &ast.Ident{Name: "TestFunc"},
				Type: &ast.FuncType{},
			}
			root := &ast.File{
				Decls: []ast.Decl{funcDecl},
			}

			astutil.Apply(root, r.replaceAstfmtCalls, nil)
			assert.Equal(t, tt.expected, comment.Text)
		})
	}
}

func TestReplaceAstfmtCalls_FuncDecl(t *testing.T) {
	tests := []struct {
		name         string
		funcName     string
		expectedName string
	}{
		{
			name:         "rename Format to FormatFast",
			funcName:     "Format",
			expectedName: "FormatFast",
		},
		{
			name:         "don't rename other functions",
			funcName:     "OtherFunc",
			expectedName: "OtherFunc",
		},
		{
			name:         "don't rename FormatString",
			funcName:     "FormatString",
			expectedName: "FormatString",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := createTestRewriter()

			funcDecl := &ast.FuncDecl{
				Name: &ast.Ident{Name: tt.funcName},
				Type: &ast.FuncType{},
			}
			root := &ast.File{
				Decls: []ast.Decl{funcDecl},
			}

			astutil.Apply(root, r.replaceAstfmtCalls, nil)
			assert.Equal(t, tt.expectedName, funcDecl.Name.Name)
		})
	}
}

func TestNewRewriter_MissingAstFormatFile(t *testing.T) {
	pkg := &packages.Package{
		Name:    "test",
		GoFiles: []string{"/path/to/other.go", "/path/to/another.go"},
		Types:   types.NewPackage("test", "test"),
	}

	// Add a dummy Expr interface to the package scope
	scope := pkg.Types.Scope()
	iface := types.NewInterfaceType(nil, nil)
	named := types.NewNamed(types.NewTypeName(0, pkg.Types, "Expr", nil), iface, nil)
	scope.Insert(types.NewTypeName(0, pkg.Types, "Expr", named))

	_, err := NewRewriter(pkg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not contain 'ast_format.go'")
}

func TestNewRewriter_Success(t *testing.T) {
	pkg := &packages.Package{
		Name:    "sqlparser",
		GoFiles: []string{"/path/to/ast_format.go", "/path/to/other.go"},
		Types:   types.NewPackage("sqlparser", "sqlparser"),
		Syntax:  []*ast.File{{}, {}},
	}

	// Add a dummy Expr interface to the package scope
	scope := pkg.Types.Scope()
	iface := types.NewInterfaceType(nil, nil)
	named := types.NewNamed(types.NewTypeName(0, pkg.Types, "Expr", nil), iface, nil)
	scope.Insert(types.NewTypeName(0, pkg.Types, "Expr", named))

	rewriter, err := NewRewriter(pkg)

	require.NoError(t, err)
	assert.NotNil(t, rewriter)
	assert.Equal(t, pkg, rewriter.pkg)
	assert.Equal(t, 0, rewriter.astfmt)
	assert.NotNil(t, rewriter.astExpr)
}

func TestReplaceAstfmtCalls_LiteralMethod(t *testing.T) {
	selIdent := &ast.Ident{Name: "literal"}
	callExpr := createCallExpr("buf", "literal", []ast.Expr{
		&ast.BasicLit{Kind: gotoken.STRING, Value: `"test"`},
	})
	callExpr.Fun.(*ast.SelectorExpr).Sel = selIdent

	exprStmt := &ast.ExprStmt{X: callExpr}
	root := wrapInFunc(exprStmt)

	r := createTestRewriter()
	r.pkg.TypesInfo.Types[selIdent] = types.TypeAndValue{
		Type: types.Typ[types.String],
	}

	astutil.Apply(root, r.replaceAstfmtCalls, nil)

	selExpr := callExpr.Fun.(*ast.SelectorExpr)
	assert.Equal(t, "WriteString", selExpr.Sel.Name)
}

func TestRewriteAstPrintf_SimpleFormats(t *testing.T) {
	tests := []struct {
		name         string
		format       string
		args         []ast.Expr
		setupTypes   func(*Rewriter, []ast.Expr)
		verifyResult func(*testing.T, *ast.BlockStmt)
	}{
		{
			name:   "literal only with single char",
			format: `"x"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"x"`},
			},
			verifyResult: func(t *testing.T, block *ast.BlockStmt) {
				require.Len(t, block.List, 1)
				stmt := block.List[0].(*ast.ExprStmt)
				call := stmt.X.(*ast.CallExpr)
				sel := call.Fun.(*ast.SelectorExpr)
				assert.Equal(t, "WriteByte", sel.Sel.Name)
				arg := call.Args[0].(*ast.BasicLit)
				assert.Equal(t, gotoken.CHAR, arg.Kind)
			},
		},
		{
			name:   "literal only with multiple chars",
			format: `"hello"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"hello"`},
			},
			verifyResult: func(t *testing.T, block *ast.BlockStmt) {
				require.Len(t, block.List, 1)
				stmt := block.List[0].(*ast.ExprStmt)
				call := stmt.X.(*ast.CallExpr)
				sel := call.Fun.(*ast.SelectorExpr)
				assert.Equal(t, "WriteString", sel.Sel.Name)
			},
		},
		{
			name:   "empty format string",
			format: `""`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `""`},
			},
			verifyResult: func(t *testing.T, block *ast.BlockStmt) {
				assert.Len(t, block.List, 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockStmt := &ast.BlockStmt{List: []ast.Stmt{}}

			astPrintfIdent := &ast.Ident{Name: "astPrintf"}
			callExpr := createCallExpr("buf", "astPrintf", tt.args)
			callExpr.Fun.(*ast.SelectorExpr).Sel = astPrintfIdent

			exprStmt := &ast.ExprStmt{X: callExpr}
			blockStmt.List = append(blockStmt.List, exprStmt)

			root := &ast.File{
				Decls: []ast.Decl{
					&ast.FuncDecl{
						Name: &ast.Ident{Name: "test"},
						Type: &ast.FuncType{},
						Body: blockStmt,
					},
				},
			}

			r := createTestRewriter()
			r.pkg.TypesInfo.Types[astPrintfIdent] = types.TypeAndValue{
				Type: types.Typ[types.String],
			}

			if tt.setupTypes != nil {
				tt.setupTypes(r, tt.args)
			}

			astutil.Apply(root, r.replaceAstfmtCalls, nil)

			if tt.verifyResult != nil {
				tt.verifyResult(t, blockStmt)
			}
		})
	}
}

func TestRewriteAstPrintf_FormatSpecifiers(t *testing.T) {
	tests := []struct {
		name         string
		format       string
		args         []ast.Expr
		setupTypes   func(*Rewriter, []ast.Expr)
		verifyResult func(*testing.T, *ast.BlockStmt)
	}{
		{
			name:   "format with %s",
			format: `"value: %s"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"value: %s"`},
				&ast.Ident{Name: "strVal"},
			},
			verifyResult: func(t *testing.T, block *ast.BlockStmt) {
				require.Len(t, block.List, 2)
				// First: WriteString("value: ")
				stmt1 := block.List[0].(*ast.ExprStmt)
				call1 := stmt1.X.(*ast.CallExpr)
				assert.Equal(t, "WriteString", call1.Fun.(*ast.SelectorExpr).Sel.Name)
				// Second: WriteString(strVal)
				stmt2 := block.List[1].(*ast.ExprStmt)
				call2 := stmt2.X.(*ast.CallExpr)
				assert.Equal(t, "WriteString", call2.Fun.(*ast.SelectorExpr).Sel.Name)
			},
		},
		{
			name:   "format with %d",
			format: `"count: %d"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"count: %d"`},
				&ast.Ident{Name: "numVal"},
			},
			verifyResult: func(t *testing.T, block *ast.BlockStmt) {
				require.Len(t, block.List, 2)
				// Second statement should be WriteString(fmt.Sprintf(...))
				stmt2 := block.List[1].(*ast.ExprStmt)
				call2 := stmt2.X.(*ast.CallExpr)
				assert.Equal(t, "WriteString", call2.Fun.(*ast.SelectorExpr).Sel.Name)
				// The arg should be a call to fmt.Sprintf
				sprintfCall := call2.Args[0].(*ast.CallExpr)
				assert.Equal(t, "fmt.Sprintf", sprintfCall.Fun.(*ast.Ident).Name)
			},
		},
		{
			name:   "format with %v (both types implement Expr interface)",
			format: `"value: %v"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"value: %v"`},
				&ast.Ident{Name: "val"},
			},
			setupTypes: func(r *Rewriter, args []ast.Expr) {
				// Both implement Expr interface, so it will call printExpr
				r.pkg.TypesInfo.Types[args[0]] = types.TypeAndValue{Type: types.NewPointer(types.Typ[types.Int])}
				r.pkg.TypesInfo.Types[args[2]] = types.TypeAndValue{Type: types.NewPointer(types.Typ[types.Int])}
			},
			verifyResult: func(t *testing.T, block *ast.BlockStmt) {
				require.Len(t, block.List, 2)
				// First: WriteString("value: ")
				// Second: printExpr call (since both implement Expr)
				stmt2 := block.List[1].(*ast.ExprStmt)
				call2 := stmt2.X.(*ast.CallExpr)
				sel := call2.Fun.(*ast.SelectorExpr)
				assert.Equal(t, "printExpr", sel.Sel.Name)
			},
		},
		{
			name:   "format with %l (left expr with both implementing Expr)",
			format: `"%l"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"%l"`},
				&ast.Ident{Name: "leftVal"},
			},
			setupTypes: func(r *Rewriter, args []ast.Expr) {
				r.pkg.TypesInfo.Types[args[0]] = types.TypeAndValue{Type: types.NewPointer(types.Typ[types.Int])}
				r.pkg.TypesInfo.Types[args[2]] = types.TypeAndValue{Type: types.NewPointer(types.Typ[types.Int])}
			},
			verifyResult: func(t *testing.T, block *ast.BlockStmt) {
				require.Len(t, block.List, 1)
				stmt := block.List[0].(*ast.ExprStmt)
				call := stmt.X.(*ast.CallExpr)
				assert.Equal(t, "printExpr", call.Fun.(*ast.SelectorExpr).Sel.Name)
				require.Len(t, call.Args, 3)
				// Third arg should be true for %l
				assert.Equal(t, "true", call.Args[2].(*ast.Ident).Name)
			},
		},
		{
			name:   "format with %r (right expr)",
			format: `"%r"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"%r"`},
				&ast.Ident{Name: "rightVal"},
			},
			setupTypes: func(r *Rewriter, args []ast.Expr) {
				r.pkg.TypesInfo.Types[args[0]] = types.TypeAndValue{Type: types.NewPointer(types.Typ[types.Int])}
				r.pkg.TypesInfo.Types[args[2]] = types.TypeAndValue{Type: types.NewPointer(types.Typ[types.Int])}
			},
			verifyResult: func(t *testing.T, block *ast.BlockStmt) {
				require.Len(t, block.List, 1)
				stmt := block.List[0].(*ast.ExprStmt)
				call := stmt.X.(*ast.CallExpr)
				assert.Equal(t, "printExpr", call.Fun.(*ast.SelectorExpr).Sel.Name)
				require.Len(t, call.Args, 3)
				// Third arg should be false for %r
				assert.Equal(t, "false", call.Args[2].(*ast.Ident).Name)
			},
		},
		{
			name:   "format with %n (slice of Expr)",
			format: `"%n"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"%n"`},
				&ast.Ident{Name: "exprs"},
			},
			setupTypes: func(r *Rewriter, args []ast.Expr) {
				sliceType := types.NewSlice(types.NewPointer(types.Typ[types.Int]))
				r.pkg.TypesInfo.Types[args[2]] = types.TypeAndValue{Type: sliceType}
			},
			verifyResult: func(t *testing.T, block *ast.BlockStmt) {
				require.Len(t, block.List, 1)
				stmt := block.List[0].(*ast.ExprStmt)
				call := stmt.X.(*ast.CallExpr)
				assert.Equal(t, "formatExprs", call.Fun.(*ast.SelectorExpr).Sel.Name)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockStmt := &ast.BlockStmt{List: []ast.Stmt{}}

			astPrintfIdent := &ast.Ident{Name: "astPrintf"}
			callExpr := createCallExpr("buf", "astPrintf", tt.args)
			callExpr.Fun.(*ast.SelectorExpr).Sel = astPrintfIdent

			exprStmt := &ast.ExprStmt{X: callExpr}
			blockStmt.List = append(blockStmt.List, exprStmt)

			root := &ast.File{
				Decls: []ast.Decl{
					&ast.FuncDecl{
						Name: &ast.Ident{Name: "test"},
						Type: &ast.FuncType{},
						Body: blockStmt,
					},
				},
			}

			r := createTestRewriter()
			r.pkg.TypesInfo.Types[astPrintfIdent] = types.TypeAndValue{
				Type: types.Typ[types.String],
			}

			if tt.setupTypes != nil {
				tt.setupTypes(r, tt.args)
			}

			astutil.Apply(root, r.replaceAstfmtCalls, nil)

			if tt.verifyResult != nil {
				tt.verifyResult(t, blockStmt)
			}
		})
	}
}

func TestRewriteAstPrintf_MultipleSpecifiers(t *testing.T) {
	tests := []struct {
		name         string
		format       string
		args         []ast.Expr
		expectedStmt int
		setupTypes   func(*Rewriter, []ast.Expr)
	}{
		{
			name:   "multiple strings: prefix %s middle %s suffix",
			format: `"name: %s, age: %s"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"name: %s, age: %s"`},
				&ast.Ident{Name: "nameVal"},
				&ast.Ident{Name: "ageVal"},
			},
			expectedStmt: 4, // "name: ", nameVal, ", age: ", ageVal
		},
		{
			name:   "mixed specifiers: %s and %d",
			format: `"name: %s, count: %d"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"name: %s, count: %d"`},
				&ast.Ident{Name: "nameVal"},
				&ast.Ident{Name: "countVal"},
			},
			expectedStmt: 4, // "name: ", nameVal, ", count: ", fmt.Sprintf("%d", countVal)
		},
		{
			name:   "consecutive format specifiers: %s%s",
			format: `"%s%s"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"%s%s"`},
				&ast.Ident{Name: "val1"},
				&ast.Ident{Name: "val2"},
			},
			expectedStmt: 2, // val1, val2
		},
		{
			name:   "format at start and end: %s middle %s",
			format: `"%s test %s"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"%s test %s"`},
				&ast.Ident{Name: "val1"},
				&ast.Ident{Name: "val2"},
			},
			expectedStmt: 3, // val1, " test ", val2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockStmt := &ast.BlockStmt{List: []ast.Stmt{}}

			astPrintfIdent := &ast.Ident{Name: "astPrintf"}
			callExpr := createCallExpr("buf", "astPrintf", tt.args)
			callExpr.Fun.(*ast.SelectorExpr).Sel = astPrintfIdent

			exprStmt := &ast.ExprStmt{X: callExpr}
			blockStmt.List = append(blockStmt.List, exprStmt)

			root := &ast.File{
				Decls: []ast.Decl{
					&ast.FuncDecl{
						Name: &ast.Ident{Name: "test"},
						Type: &ast.FuncType{},
						Body: blockStmt,
					},
				},
			}

			r := createTestRewriter()
			r.pkg.TypesInfo.Types[astPrintfIdent] = types.TypeAndValue{
				Type: types.Typ[types.String],
			}

			if tt.setupTypes != nil {
				tt.setupTypes(r, tt.args)
			}

			astutil.Apply(root, r.replaceAstfmtCalls, nil)

			assert.Len(t, blockStmt.List, tt.expectedStmt)
		})
	}
}

func TestRewriteAstPrintf_PanicCases(t *testing.T) {
	tests := []struct {
		name       string
		format     string
		args       []ast.Expr
		setupTypes func(*Rewriter, []ast.Expr)
		panicMsg   string
	}{
		{
			name:   "unsupported format specifier %x",
			format: `"%x"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"%x"`},
				&ast.Ident{Name: "val"},
			},
			panicMsg: `unsupported escape 'x'`,
		},
		{
			name:   "%n with non-slice type",
			format: `"%n"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"%n"`},
				&ast.Ident{Name: "val"},
			},
			setupTypes: func(r *Rewriter, args []ast.Expr) {
				r.pkg.TypesInfo.Types[args[2]] = types.TypeAndValue{Type: types.Typ[types.Int]}
			},
			panicMsg: "'%n' directive requires a slice",
		},
		{
			name:   "invalid literal argument (non-unquotable)",
			format: `"test`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"test`}, // Invalid: missing closing quote
			},
			panicMsg: "bad literal argument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockStmt := &ast.BlockStmt{List: []ast.Stmt{}}

			astPrintfIdent := &ast.Ident{Name: "astPrintf"}
			callExpr := createCallExpr("buf", "astPrintf", tt.args)
			callExpr.Fun.(*ast.SelectorExpr).Sel = astPrintfIdent

			exprStmt := &ast.ExprStmt{X: callExpr}
			blockStmt.List = append(blockStmt.List, exprStmt)

			root := &ast.File{
				Decls: []ast.Decl{
					&ast.FuncDecl{
						Name: &ast.Ident{Name: "test"},
						Type: &ast.FuncType{},
						Body: blockStmt,
					},
				},
			}

			r := createTestRewriter()
			r.pkg.TypesInfo.Types[astPrintfIdent] = types.TypeAndValue{
				Type: types.Typ[types.String],
			}

			if tt.setupTypes != nil {
				tt.setupTypes(r, tt.args)
			}

			assert.PanicsWithValue(t, tt.panicMsg, func() {
				astutil.Apply(root, r.replaceAstfmtCalls, nil)
			})
		})
	}
}

func TestRewriteAstPrintf_EdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		format       string
		args         []ast.Expr
		setupTypes   func(*Rewriter, []ast.Expr)
		verifyResult func(*testing.T, *ast.BlockStmt)
	}{
		{
			name:   "single character literal",
			format: `"("`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"("`},
			},
			verifyResult: func(t *testing.T, block *ast.BlockStmt) {
				require.Len(t, block.List, 1)
				stmt := block.List[0].(*ast.ExprStmt)
				call := stmt.X.(*ast.CallExpr)
				assert.Equal(t, "WriteByte", call.Fun.(*ast.SelectorExpr).Sel.Name)
			},
		},
		{
			name:   "format specifier at start",
			format: `"%s suffix"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"%s suffix"`},
				&ast.Ident{Name: "val"},
			},
			verifyResult: func(t *testing.T, block *ast.BlockStmt) {
				require.Len(t, block.List, 2)
			},
		},
		{
			name:   "format specifier at end",
			format: `"prefix %s"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"prefix %s"`},
				&ast.Ident{Name: "val"},
			},
			verifyResult: func(t *testing.T, block *ast.BlockStmt) {
				require.Len(t, block.List, 2)
			},
		},
		{
			name:   "hash modifier on non-v specifier (%#s should still use WriteString)",
			format: `"%#s"`,
			args: []ast.Expr{
				&ast.Ident{Name: "node"},
				&ast.BasicLit{Kind: gotoken.STRING, Value: `"%#s"`},
				&ast.Ident{Name: "val"},
			},
			verifyResult: func(t *testing.T, block *ast.BlockStmt) {
				require.Len(t, block.List, 1)
				stmt := block.List[0].(*ast.ExprStmt)
				call := stmt.X.(*ast.CallExpr)
				assert.Equal(t, "WriteString", call.Fun.(*ast.SelectorExpr).Sel.Name)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockStmt := &ast.BlockStmt{List: []ast.Stmt{}}

			astPrintfIdent := &ast.Ident{Name: "astPrintf"}
			callExpr := createCallExpr("buf", "astPrintf", tt.args)
			callExpr.Fun.(*ast.SelectorExpr).Sel = astPrintfIdent

			exprStmt := &ast.ExprStmt{X: callExpr}
			blockStmt.List = append(blockStmt.List, exprStmt)

			root := &ast.File{
				Decls: []ast.Decl{
					&ast.FuncDecl{
						Name: &ast.Ident{Name: "test"},
						Type: &ast.FuncType{},
						Body: blockStmt,
					},
				},
			}

			r := createTestRewriter()
			r.pkg.TypesInfo.Types[astPrintfIdent] = types.TypeAndValue{
				Type: types.Typ[types.String],
			}

			if tt.setupTypes != nil {
				tt.setupTypes(r, tt.args)
			}

			astutil.Apply(root, r.replaceAstfmtCalls, nil)

			if tt.verifyResult != nil {
				tt.verifyResult(t, blockStmt)
			}
		})
	}
}
