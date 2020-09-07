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

// TestExpirePeriod checks that the set expire period is returned
func TestExpirePeriod(t *testing.T) {
	oneSecond := time.Second
	twoSeconds := 2 * oneSecond

	// create a new collection
	c := &Collection{}

	// check if we change it we get back the value we provided
	c.SetExpirePeriod(oneSecond)
	if c.ExpirePeriod() != oneSecond {
		t.Errorf("TestExpirePeriod: did not get back oneSecond")
	}

	// change the period and check again
	c.SetExpirePeriod(twoSeconds)
	if c.ExpirePeriod() != twoSeconds {
		t.Errorf("TestExpirePeriod: did not get back twoSeconds")
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

	if len(c.Metrics()) != 0 {
		t.Errorf("TestAppend: len(Metrics) = %d, expecting %d", len(c.Metrics()), 0)
	}
	for _, v := range []int{1, 2, 3} {
		tm := &testMetric{}
		c.Append(tm)
		if len(c.Metrics()) != v {
			t.Errorf("TestExpirePeriod: len(Metrics) = %d, expecting %d", len(c.Metrics()), v)
		}
	}
}
