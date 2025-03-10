/*
   Copyright 2017 Simon J Mudd

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

package collection

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var randomString = []string{
	"RANDOM_STRING",
	"SOME_OTHER_STRING",
}

// some random base timestamp
var ts = time.Date(2016, 12, 27, 13, 36, 40, 0, time.Local)

// TestCreateOrReturn tests the creation of a named Collection
func TestCreateOrReturnCollection(t *testing.T) {
	name := randomString[0]
	// check we get the same reference with a single name
	c1 := CreateOrReturnCollection(name)
	if c1 == nil {
		// should not be empty
		t.Errorf("TestCreateOrReturn: c1 == nil, name=%s", name)
	}
	c2 := CreateOrReturnCollection(name)
	if c2 == nil || c2 != c1 {
		t.Errorf("TestCreateOrReturn: c2 == nil || c2 != c1")
		// should not be empty, or different to c1
	}

	name = randomString[1]
	// check we get a new reference and it's different to what we had before
	c3 := CreateOrReturnCollection(name)
	if c3 == nil || c3 == c1 {
		// should not be empty, or same as c1
		t.Errorf("TestCreateOrReturn: c3 == nil || c3 == c1")
	}
	c4 := CreateOrReturnCollection(name)
	// check our reference matches c3 but not c2/c1
	if c4 == nil || c4 != c3 || c4 == c2 {
		t.Errorf("TestCreateOrReturn: c3 == nil || c4 != c3 || c4 == c2")
	}
}

// dummy structure for testing
type testMetric struct {
}

func (tm *testMetric) When() time.Time {
	return ts
}

// check that Append() works as expected
func TestAppend(t *testing.T) {
	c := &Collection{}
	// Test for nil metric
	err := c.Append(nil)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "Collection.Append: m == nil")
}
