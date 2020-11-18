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

package main

import (
	"flag"
	"os"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgategate"
)

func init() {
	servenv.RegisterDefaultFlags()
}

func main() {
	flag.Parse()

	if *servenv.Version {
		servenv.AppVersion.Print()
		os.Exit(0)
	}

	if len(flag.Args()) > 0 {
		flag.Usage()
		log.Exit("vtgategate doesn't take any positional arguments")
	}

	servenv.Init()

	err := vtgategate.Init()
	if err != nil {
		log.Exitf("error initializing proxy: %v", err)
	}

	servenv.RunDefault()
}
