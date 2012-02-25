/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strconv"
	"strings"
)

var fset = token.NewFileSet()
var iprefix = flag.String("iprefix", "vitess/", "Match prefix to include in deps")
var depsPrefix = flag.String("dprefix", "$(GOTOP)/", "Prefix all generated deps with this")
var outfile = flag.String("o", "Make.deps", "Dependency file")

func main() {
	flag.Parse()
	deps := make(map[string]bool)
	for i := 0; i < flag.NArg(); i++ {
		if err := extract(flag.Arg(i), *iprefix, deps); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		}
	}
	sorted := sortDeps(deps)
	if err := generate(sorted); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func extract(filename, prefix string, deps map[string]bool) error {
	file, err := parser.ParseFile(fset, filename, nil, parser.ImportsOnly)
	if err != nil {
		return err
	}
	for _, s := range file.Imports {
		if result := importMatch(s, prefix); result != "" {
			deps[result] = true
		}
	}
	return nil
}

func importMatch(s *ast.ImportSpec, prefix string) string {
	t, err := strconv.Unquote(s.Path.Value)
	if err != nil {
		return ""
	}
	if !strings.HasPrefix(t, prefix) {
		return ""
	}
	return t[len(prefix):]
}

func sortDeps(deps map[string]bool) (out []string) {
	out = make([]string, len(deps))
	index := 0
	for k := range deps {
		out[index] = k
		index++
	}
	sort.Strings(out)
	return out
}

func generate(sorted []string) error {
	out, err := os.Create(*outfile)
	if err != nil {
		return err
	}
	defer out.Close()
	if len(sorted) == 0 {
		return nil
	}
	fmt.Fprintf(out, "DEPS=\\\n")
	for _, v := range sorted {
		fmt.Fprintf(out, "\t%s%s\\\n", *depsPrefix, v)
	}
	fmt.Fprintf(out, "\n")
	return nil
}
