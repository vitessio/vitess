// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Import and register the gorpc mysqlctl server

import (
	"github.com/youtube/vitess/go/vt/mysqlctl/gorpcmysqlctlserver"
	"github.com/youtube/vitess/go/vt/servenv"
)

func init() {
	servenv.OnRun(func() {
		gorpcmysqlctlserver.StartServer(mysqld)
	})
}
