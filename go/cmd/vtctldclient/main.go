/*
Copyright 2020 The Vitess Authors.

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

	"vitess.io/vitess/go/cmd/vtctldclient/internal/command"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/log"
)

func main() {
	defer exit.Recover()

	// Grab all those global flags across the codebase and shove 'em on in.
	command.Root.PersistentFlags().AddGoFlagSet(flag.CommandLine)

	// hack to get rid of an "ERROR: logging before flag.Parse"
	args := os.Args[:]
	os.Args = os.Args[:1]
	flag.Parse()
	os.Args = args

	// back to your regularly scheduled cobra programming
	if err := command.Root.Execute(); err != nil {
		log.Error(err)
		exit.Return(1)
	}
}
