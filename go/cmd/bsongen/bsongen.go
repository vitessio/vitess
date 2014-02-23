// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/ioutil"
	"log"

	"github.com/youtube/vitess/go/testfiles"
)

func main() {
	input := testfiles.Locate("bson_test/simple_type.go")
	bytes, err := ioutil.ReadFile(input)
	if err != nil {
		log.Fatal(err)
	}
	src := string(bytes)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", src, 0)
	if err != nil {
		log.Fatal(err)
	}
	ast.Print(fset, f)
}
