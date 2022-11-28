/*
Copyright 2022 The Vitess Authors.

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
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
)

type Test struct {
	Args []string
}

type Config struct {
	Tests map[string]*Test
}

func main() {
	content, err := os.ReadFile("./test/config.json")
	if err != nil {
		log.Fatal(err)
	}

	tests := &Config{}
	err = json.Unmarshal(content, tests)
	if err != nil {
		log.Fatal(err)
	}

	var failedConfig []string
	for name, test := range tests.Tests {
		if len(test.Args) == 0 {
			continue
		}
		path := test.Args[0]
		if !strings.HasPrefix(path, "vitess.io/vitess/") {
			continue
		}
		path = path[len("vitess.io/vitess/"):]

		stat, err := os.Stat(path)
		if err != nil || !stat.IsDir() {
			failedConfig = append(failedConfig, fmt.Sprintf("%s: %s", name, path))
			continue
		}
	}

	if len(failedConfig) > 0 {
		fmt.Println("Some packages in test/config.json were not found in the codebase:")
		for _, failed := range failedConfig {
			fmt.Println("\t" + failed)
		}
		fmt.Println("\nYou must remove them from test/config.json to avoid unnecessary CI load.")
		os.Exit(1)
	}
	fmt.Println("The file: test/config.json is clean.")
}
