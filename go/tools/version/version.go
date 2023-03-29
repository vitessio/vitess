/*
Copyright 2023 The Vitess Authors.

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
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
)

// This program reads the Vitess version of the current git branch and prints it on the standard output.
func main() {
	file, err := os.OpenFile("./go/vt/servenv/version.go", os.O_RDONLY, 0640)
	if err != nil {
		log.Fatal(err)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}
	contentStr := string(content)

	rxp := regexp.MustCompile(`const[[:space:]]versionName[[:space:]]=[[:space:]]\"([0-9.]+)(-SNAPSHOT)?\"`)
	m := rxp.FindStringSubmatch(contentStr)
	if len(m) != 3 {
		log.Fatal("Could not find the version, got:", m)
	}

	// Print the version i.e. "17.0.0"
	fmt.Println(m[1])
}
