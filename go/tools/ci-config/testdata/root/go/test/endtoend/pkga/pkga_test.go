// Copyright 2026 The Vitess Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pkga

import "testing"

func TestFoo(t *testing.T) {}

func TestBar(t *testing.T) {}

// TestMain with *testing.M is the package's test entrypoint, not a test.
func TestMain(m *testing.M) {}

// testHelper does not have the Test prefix.
func testHelper(t *testing.T) {}

// TestWrongSig has more than one parameter.
func TestWrongSig(t *testing.T, x int) {}

// TestNoParams has no parameters.
func TestNoParams() {}

// TestWithResult returns a value.
func TestWithResult(t *testing.T) error { return nil }

func BenchmarkFoo(b *testing.B) {}

type thing struct{}

// TestMethod has a receiver.
func (th *thing) TestMethod(t *testing.T) {}
