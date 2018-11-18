/*
Copyright 2018 The Vitess Authors.

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

package planbuilder

import (
	"reflect"
	"testing"
)

func TestGenerateSubqueryVars(t *testing.T) {
	jt := newJointab(map[string]struct{}{
		"__sq1":            {},
		"__sq_has_values3": {},
	})

	v1, v2 := jt.GenerateSubqueryVars()
	combined := []string{v1, v2}
	want := []string{"__sq2", "__sq_has_values2"}
	if !reflect.DeepEqual(combined, want) {
		t.Errorf("jt.GenerateSubqueryVars: %v, want %v", combined, want)
	}

	v1, v2 = jt.GenerateSubqueryVars()
	combined = []string{v1, v2}
	want = []string{"__sq4", "__sq_has_values4"}
	if !reflect.DeepEqual(combined, want) {
		t.Errorf("jt.GenerateSubqueryVars: %v, want %v", combined, want)
	}
}
