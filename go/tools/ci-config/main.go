/*
Copyright 2026 The Vitess Authors.

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

// Command ci-config validates the CI end-to-end test configuration in
// test/config*.json against the test functions that actually exist in the
// tree, so that tests cannot be silently orphaned by a stale or mistyped
// -run regex. See https://github.com/vitessio/vitess/issues/20261.
//
// It must be run from the repository root.
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func main() {
	// The same glob test.go uses to load the configs.
	configPaths, err := filepath.Glob("test/config*.json")
	if err != nil {
		log.Fatal(err)
	}
	if len(configPaths) == 0 {
		log.Fatal("no test/config*.json files found; run this tool from the repository root")
	}

	problems := run(".", configPaths)
	if len(problems) > 0 {
		fmt.Println("Problems found in the CI test configuration:")
		for _, problem := range problems {
			fmt.Println("\t" + problem)
		}
		os.Exit(1)
	}
	fmt.Println("The CI test configuration is clean.")
}
