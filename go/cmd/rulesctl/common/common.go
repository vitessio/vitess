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

package common

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	vtfcr "vitess.io/vitess/go/vt/vttablet/customrule/filecustomrule"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

func GetRules(path string) *rules.Rules {
	rules, err := vtfcr.ParseRules(path)
	if err != nil {
		log.Fatalf("Failure attempting to parse rules: %v", err)
	}
	return rules
}

func MustPrintJSON(obj any) {
	enc, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		log.Fatalf("Unable to marshal object: %v", err)
	}
	fmt.Printf("%v\n", string(enc))
}

func MustWriteJSON(obj any, path string) {
	enc, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		log.Fatalf("Unable to marshal object: %v", err)
	}

	err = os.WriteFile(path, enc, 0o400)
	if err != nil {
		log.Fatalf("Unable to save new JSON: %v", err)
	}
}
