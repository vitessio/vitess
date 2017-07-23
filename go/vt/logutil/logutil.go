/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// package logutil provides some utilities for logging using glog and
// redirects the stdlib logging to glog.

package logutil

import (
	stdlog "log"

	log "github.com/golang/glog"
)

type logShim struct{}

func (shim *logShim) Write(buf []byte) (n int, err error) {
	log.Info(string(buf))
	return len(buf), nil
}

func init() {
	stdlog.SetPrefix("log: ")
	stdlog.SetFlags(0)
	stdlog.SetOutput(new(logShim))
}
