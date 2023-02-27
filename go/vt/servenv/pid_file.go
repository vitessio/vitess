/*
Copyright 2019 The Vitess Authors.

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
	"fmt"
	"os"

	"vitess.io/vitess/go/vt/log"
)

var pidFile string // registered in RegisterFlags as --pid_file

func init() {
	pidFileCreated := false

	// Create pid file after flags are parsed.
	OnInit(func() {
		if pidFile == "" {
			return
		}

		file, err := os.OpenFile(pidFile, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			log.Errorf("Unable to create pid file '%s': %v", pidFile, err)
			return
		}
		pidFileCreated = true
		fmt.Fprintln(file, os.Getpid())
		file.Close()
	})

	// Remove pid file on graceful shutdown.
	OnClose(func() {
		if pidFile == "" {
			return
		}
		if !pidFileCreated {
			return
		}

		if err := os.Remove(pidFile); err != nil {
			log.Errorf("Unable to remove pid file '%s': %v", pidFile, err)
		}
	})
}
