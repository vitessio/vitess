// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/youtube/vitess/go/testfiles"
)

func TestSimple(t *testing.T) {
	input := testfiles.Locate("bson_test/simple_type.go")
	b, err := ioutil.ReadFile(input)
	if err != nil {
		log.Fatal(err)
	}
	out, err := generateCode(string(b), "MyType")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}
	fmt.Printf("%s\n", out)
}
