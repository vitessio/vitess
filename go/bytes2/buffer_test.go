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

package bytes2

import (
	"testing"
)

func TestBuffer(t *testing.T) {
	b := NewBuffer(nil)
	if _, err := b.Write([]byte("ab")); err != nil {
		t.Errorf("error in writing : %v", err)
	}
	if _, err := b.WriteString("cd"); err != nil {
		t.Errorf("error in writing string : %v", err)
	}
	if err := b.WriteByte('e'); err != nil {
		t.Errorf("error in writing byte : %v", err)
	}

	want := "abcde"
	if got := string(b.Bytes()); got != want {
		t.Errorf("b.Bytes(): %s, want %s", got, want)
	}
	if got := b.String(); got != want {
		t.Errorf("b.String(): %s, want %s", got, want)
	}
	if got := b.Len(); got != 5 {
		t.Errorf("b.Len(): %d, want 5", got)
	}
}
