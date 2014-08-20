// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Imports and register the gorpc vtctl server

import (
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vtctl/gorpcvtctlserver"
)

func init() {
	servenv.OnRun(func() {
		gorpcvtctlserver.StartServer(ts)
	})
}
