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

package fuzzing

import (
	fuzz "github.com/AdaLogics/go-fuzz-headers"

	"vitess.io/vitess/go/mysql/collations"
)

func FuzzCollateUnicode(data []byte) int {
	c := &collations.Collation_unicode_general_ci{}
	f := fuzz.NewConsumer(data)
	err := f.GenerateStruct(c)
	if err != nil {
		return 0
	}
	left, err := f.GetBytes()
	if err != nil {
		return 0
	}
	right, err := f.GetBytes()
	if err != nil {
		return 0
	}
	isPrefix, err := f.GetBool()
	if err != nil {
		return 0
	}
	_ = c.Collate(left, right, isPrefix)
	return 1
}

func FuzzCollateWildcard(data []byte) int {
	c := &collations.Collation_multibyte{}
	f := fuzz.NewConsumer(data)
	err := f.GenerateStruct(c)
	if err != nil {
		return 0
	}
	pat, err := f.GetBytes()
	if err != nil {
		return 0
	}
	in, err := f.GetBytes()
	if err != nil {
		return 0
	}
	wcp := c.Wildcard(pat, 0, 0, 0)
	wcp.Match(in)
	return 1
}
