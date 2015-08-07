// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package main is the implementation of vtgateclienttest.
// This program has a chain of vtgateservice.VTGateService implementations,
// each one being responsible for one test scenario.
package main

import (
	"flag"

	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
)

func init() {
	servenv.RegisterDefaultFlags()
}

// createService creates the implementation chain of all the test cases
func createService() vtgateservice.VTGateService {
	var s vtgateservice.VTGateService
	s = newTerminalClient()
	s = newSuccessClient(s)
	s = newErrorClient(s)
	s = newCallerIDClient(s)
	return s
}

func main() {
	defer exit.Recover()

	flag.Parse()
	servenv.Init()

	// The implementation chain.
	s := createService()
	for _, f := range vtgate.RegisterVTGates {
		f(s)
	}

	servenv.RunDefault()
}
