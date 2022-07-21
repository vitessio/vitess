/*
Copyright 2021 The Vitess Authors.

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

package common

import (
	"log"

	"golang.org/x/tools/go/packages"
)

// PkgFailed returns true if any of the packages contain errors
func PkgFailed(loaded []*packages.Package) bool {
	failed := false
	for _, pkg := range loaded {
		for _, e := range pkg.Errors {
			log.Println(e.Error())
			failed = true
		}
	}
	return failed
}
