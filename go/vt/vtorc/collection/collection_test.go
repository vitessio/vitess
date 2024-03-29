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
var ts2 = ts.AddDate(-1, 0, 0)

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

type testMetric2 struct {
}

func (tm *testMetric2) When() time.Time {
	return ts2
}

// check that Append() works as expected
func TestAppend(t *testing.T) {
	c := &Collection{}
	// Test for nil metric
	err := c.Append(nil)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "Collection.Append: m == nil")
}

func TestNilCollection(t *testing.T) {
	var c *Collection

	err := c.Append(nil)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "Collection.Append: c == nil")

	err = c.removeBefore(ts)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "Collection.removeBefore: c == nil")

	// Should not throw any error for nil Collection
	c.StartAutoExpiration()
	c.StopAutoExpiration()
}

func TestStopAutoExpiration(t *testing.T) {
	oldNamedCollection := namedCollection
	defer func() {
		namedCollection = oldNamedCollection
	}()
	// Clear Collection map
	namedCollection = make(map[string]*Collection)

	name := randomString[0]
	c := CreateOrReturnCollection(name)

	c.StopAutoExpiration()
	assert.False(t, c.monitoring)

	// Test when c.monitoring == true before calling StartAutoExpiration
	c.monitoring = true
	c.StartAutoExpiration()
	assert.True(t, c.monitoring)
}

func TestSince(t *testing.T) {
	oldNamedCollection := namedCollection
	defer func() {
		namedCollection = oldNamedCollection
	}()
	// Clear Collection map
	namedCollection = make(map[string]*Collection)

	name := randomString[0]

	var c *Collection
	metrics, err := c.Since(ts)

	assert.Nil(t, metrics)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "Collection.Since: c == nil")

	c = CreateOrReturnCollection(name)
	metrics, err = c.Since(ts)
	assert.Nil(t, metrics)
	assert.Nil(t, err)

	tm := &testMetric{}
	tm2 := &testMetric2{}
	_ = c.Append(tm2)
	_ = c.Append(tm)

	metrics, err = c.Since(ts2)
	assert.Equal(t, []Metric{tm2, tm}, metrics)
	assert.Nil(t, err)

	metrics, err = c.Since(ts)
	assert.Equal(t, []Metric{tm}, metrics)
	assert.Nil(t, err)
}

func TestRemoveBefore(t *testing.T) {
	oldNamedCollection := namedCollection
	defer func() {
		namedCollection = oldNamedCollection
	}()
	// Clear Collection map
	namedCollection = make(map[string]*Collection)

	name := randomString[0]
	c := CreateOrReturnCollection(name)

	tm := &testMetric{}
	tm2 := &testMetric2{}

	err := c.Append(tm2)
	assert.Nil(t, err)

	err = c.Append(tm)
	assert.Nil(t, err)

	err = c.removeBefore(ts)
	assert.NoError(t, err)
	assert.Equal(t, []Metric{tm}, c.collection)

	ts3 := ts.AddDate(1, 0, 0)
	err = c.removeBefore(ts3)
	assert.NoError(t, err)
	assert.Nil(t, c.collection)

	name = randomString[1]
	c = CreateOrReturnCollection(name)

	err = c.removeBefore(ts)
	assert.NoError(t, err)
	assert.Equal(t, []Metric(nil), c.collection)
}
