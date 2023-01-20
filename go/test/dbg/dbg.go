/*
Copyright 2023 The Vitess Authors

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

package dbg

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"

	"github.com/kr/pretty"
	"github.com/kr/text"
)

type params struct {
	pos    token.Position
	fn     string
	params []string
}

func (p *params) Position() string {
	if p == nil {
		return "<unknown>"
	}
	return p.pos.String()
}

func (p *params) ShortPosition() string {
	if p == nil {
		return "<unknown>"
	}
	return fmt.Sprintf("%s:%d", path.Base(p.pos.Filename), p.pos.Line)
}

func (p *params) Arg(n int) string {
	if p == nil || n >= len(p.params) {
		return "arg"
	}
	return p.params[n]
}

func (p *params) Fn() string {
	if p == nil || p.fn == "" {
		return "?"
	}
	return p.fn
}

type file struct {
	once  sync.Once
	fset  *token.FileSet
	path  string
	calls map[int]*params
}

type cache struct {
	mu     sync.Mutex
	parsed map[string]*file
	fset   *token.FileSet
}

func (f *file) parse() {
	a, err := parser.ParseFile(f.fset, f.path, nil, 0)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "[dbg] failed to parse %q: %v\n", f.path, err)
		return
	}

	f.calls = map[int]*params{}

	var curfn string
	ast.Inspect(a, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.FuncDecl:
			var buf strings.Builder
			if n.Recv != nil && len(n.Recv.List) == 1 {
				buf.WriteByte('(')
				_ = format.Node(&buf, f.fset, n.Recv.List[0].Type)
				buf.WriteString(").")
			}
			buf.WriteString(n.Name.String())
			curfn = buf.String()

		case *ast.CallExpr:
			if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
				if pkg, ok := sel.X.(*ast.Ident); ok {
					if pkg.Name == "dbg" && (sel.Sel.Name == "P" || sel.Sel.Name == "V") {
						var p = params{
							pos: f.fset.Position(n.Pos()),
							fn:  curfn,
						}

						for _, arg := range n.Args {
							var buf strings.Builder
							_ = format.Node(&buf, f.fset, arg)
							p.params = append(p.params, buf.String())
						}

						f.calls[p.pos.Line] = &p
						return false
					}
				}
			}
		}
		return true
	})
}

func (f *file) resolve(lineno int) *params {
	f.once.Do(f.parse)
	return f.calls[lineno]
}

func (c *cache) resolve(filename string, lineno int) *params {
	var f *file

	c.mu.Lock()
	f = c.parsed[filename]
	if f == nil {
		f = &file{fset: c.fset, path: filename}
		c.parsed[filename] = f
	}
	c.mu.Unlock()

	return f.resolve(lineno)
}

var defaultCache = cache{
	fset:   token.NewFileSet(),
	parsed: map[string]*file{},
}

// V prints the given argument in compact debug form and returns it unchanged
func V[Val any](v Val) Val {
	var p *params
	if _, f, lineno, ok := runtime.Caller(1); ok {
		p = defaultCache.resolve(f, lineno)
	}
	_, _ = fmt.Fprintf(os.Stdout, "[%s]: %s = %# v\n", p.ShortPosition(), p.Arg(0), pretty.Formatter(v))
	return v
}

// P prints all the arguments passed to the function in verbose debug form
func P(vals ...any) {
	var p *params
	if _, f, lineno, ok := runtime.Caller(1); ok {
		p = defaultCache.resolve(f, lineno)
	}

	var buf bytes.Buffer
	_, _ = fmt.Fprintf(&buf, "%s @ %s\n", p.Position(), p.Fn())
	for i, v := range vals {
		indent, _ := fmt.Fprintf(&buf, "    [%d] %s = ", i, p.Arg(i))

		w := text.NewIndentWriter(&buf, nil, bytes.Repeat([]byte{' '}, indent))
		fmt.Fprintf(w, "%# v\n", pretty.Formatter(v))
	}
	_, _ = buf.WriteTo(os.Stdout)
}
