// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"flag"
	"os"
	"runtime/pprof"

	log "github.com/golang/glog"
)

var (
	cpuProfile = flag.String("cpu_profile", "", "write cpu profile to file")
)

func init() {
	onInit(func() {
		if *cpuProfile != "" {
			f, err := os.Create(*cpuProfile)
			if err != nil {
				log.Fatalf("Failed to create profile file: %v", err)
			}
			pprof.StartCPUProfile(f)
			OnTerm(func() {
				pprof.StopCPUProfile()
			})
		}
	})
}
