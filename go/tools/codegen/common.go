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

package codegen

import (
	"fmt"
	"path"
	"strings"

	"golang.org/x/tools/go/packages"
)

func CheckErrors(loaded []*packages.Package, skip func(fileName string) bool) error {
	var errors []string
	for _, l := range loaded {
		for _, e := range l.Errors {
			idx := strings.Index(e.Pos, ":")
			filePath := e.Pos[:idx]
			_, fileName := path.Split(filePath)
			if !skip(fileName) {
				errors = append(errors, e.Error())
			}
		}
	}
	if len(errors) > 0 {
		return fmt.Errorf("found %d error(s) when loading Go packages:\n\t%s", len(errors), strings.Join(errors, "\n\t"))
	}
	return nil
}

func GeneratedInSqlparser(filename string) bool {
	switch filename {
	case "ast_format_fast.go": // astfmtgen
		return true
	case "ast_equals.go", "ast_clone.go", "ast_rewrite.go", "ast_visit.go", "ast_copy_on_rewrite.go": // asthelpergen
		return true
	default:
		return false
	}
}
