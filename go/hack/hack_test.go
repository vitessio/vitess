/*
Copyright 2019 The Vitess Authors.

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

package hack

import (
	"testing"
)

func TestByteToString(t *testing.T) {
	v1 := []byte("1234")
	if s := String(v1); s != "1234" {
		t.Errorf("String(\"1234\"): %q, want 1234", s)
	}

	v1 = []byte("")
	if s := String(v1); s != "" {
		t.Errorf("String(\"\"): %q, want empty", s)
	}

	v1 = nil
	if s := String(v1); s != "" {
		t.Errorf("String(\"\"): %q, want empty", s)
	}
}
