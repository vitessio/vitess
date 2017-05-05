/*
Copyright 2017 Google Inc.

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
