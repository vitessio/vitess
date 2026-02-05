/*
Copyright 2024 The Vitess Authors.

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

package tb

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStackTrace(t *testing.T) {
	testErr := "test err"
	testStackTrace := "test stack trace"
	testStackError := stackError{
		err:        fmt.Errorf("%s", testErr),
		stackTrace: testStackTrace,
	}

	expectedErr := fmt.Sprintf("%s\n%s", testErr, testStackTrace)
	assert.Equal(t, expectedErr, testStackError.Error())
	assert.Equal(t, testStackTrace, testStackError.StackTrace())
}

func TestStack(t *testing.T) {
	// skip is set to 2 to check if the 3rd function in
	// the go routine stack is called from this file
	// 1st func is expected to be stack and 2nd to be Stack
	b := Stack(2)
	l := bytes.Split(b, []byte(":"))

	_, file, _, _ := runtime.Caller(0)
	assert.Equal(t, string(l[0]), file)
}

func TestFunction(t *testing.T) {
	pc, _, _, _ := runtime.Caller(0)
	name := function(pc)

	assert.Equal(t, "io/vitess/go/tb.TestFunction", string(name))

	// invalid program counter
	name = function(0)
	assert.Equal(t, name, dunno)
}

func TestErrorf(t *testing.T) {
	s1 := stackError{
		err:        fmt.Errorf("err1"),
		stackTrace: "stackTrace1",
	}
	s2 := stackError{
		err:        fmt.Errorf("err2"),
		stackTrace: "stackTrace2",
	}
	err := Errorf("test msg %v %v", s1, s2)

	expectedMsg := fmt.Sprintf("test msg %v %v", s1, s2)
	expectedErr := fmt.Sprintf("%v\n%v", expectedMsg, "stackTrace1")
	assert.Equal(t, err.Error(), expectedErr)

	err = Errorf("test msg")
	s := string(Stack(4))
	expectedErr = fmt.Sprintf("%v\n%v", "test msg", s)
	assert.Equal(t, err.Error(), expectedErr)
}

func TestSource(t *testing.T) {
	lines := [][]byte{
		[]byte("\ttest line 1\t"),
		[]byte("\ttest line 2\t"),
		[]byte("\ttest line 3\t"),
	}

	assert.Equal(t, []byte("test line 1"), source(lines, 0))
	assert.Equal(t, []byte("test line 2"), source(lines, 1))
	assert.Equal(t, dunno, source(lines, -1))
	assert.Equal(t, dunno, source(lines, 3))
}
