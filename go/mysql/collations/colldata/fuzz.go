//go:build gofuzz
// +build gofuzz

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

package colldata

import (
	fuzz "github.com/AdaLogics/go-fuzz-headers"
)

var (
	collations = []string{"utf8mb4_bin", "utf8mb4_0900_ai_ci",
		"utf8mb4_0900_as_ci", "utf8mb4_0900_as_cs",
		"utf8mb4_0900_ai_ci", "utf8mb4_0900_as_ci",
		"utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci",
		"utf8mb4_hu_0900_as_cs", "utf8mb4_ja_0900_as_cs",
		"utf8mb4_ja_0900_as_cs_ks", "utf8mb4_zh_0900_as_cs",
		"utf8mb4_zh_0900_as_cs"}
)

func FuzzCollations(data []byte) int {
	testinit()
	f := fuzz.NewConsumer(data)
	collIndex, err := f.GetInt()
	if err != nil {
		return 0
	}
	collString := collations[collIndex%len(collations)]
	coll := testcollationMap[collString]
	left, err := f.GetBytes()
	if err != nil {
		return 0
	}
	right, err := f.GetBytes()
	if err != nil {
		return 0
	}
	_ = coll.Collate(left, right, false)
	return 1
}
