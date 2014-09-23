// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"flag"
	"fmt"
	"os"

	log "github.com/golang/glog"
)

var pidFile = flag.String("pid_file", "", "If set, the process will write its pid to the named file, and delete it on graceful shutdown.")

func init() {
	// Create pid file after flags are parsed.
	onInit(func() {
		if *pidFile == "" {
			return
		}

		file, err := os.Create(*pidFile)
		if err != nil {
			log.Errorf("Unable to create pid file '%s': %v", *pidFile, err)
			return
		}
		fmt.Fprintln(file, os.Getpid())
		file.Close()
	})

	// Remove pid file on graceful shutdown.
	OnClose(func() {
		if *pidFile == "" {
			return
		}

		if err := os.Remove(*pidFile); err != nil {
			log.Errorf("Unable to remove pid file '%s': %v", *pidFile, err)
		}
	})
}
