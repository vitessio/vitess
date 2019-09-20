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
	"flag"
	"os"
	"runtime/pprof"

	"vitess.io/vitess/go/vt/log"
)

var (
	cpuProfile = flag.String("cpu_profile", "", "write cpu profile to file")
)

func init() {
	OnInit(func() {
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
