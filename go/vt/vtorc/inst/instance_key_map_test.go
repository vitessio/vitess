/*
   Copyright 2014 Outbrain Inc.

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

package inst

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtorc/config"
)

func init() {
	config.MarkConfigurationLoaded()
}

func TestGetInstanceKeys(t *testing.T) {
	for range rand.Perm(10) { // Just running many iterations to cover multiple possible map iteration ordering. Perm() is just used as an array generator here.
		m := *NewInstanceKeyMap()
		m.AddKey(key1)
		m.AddKey(key2)
		keys := m.GetInstanceKeys()
		require.Equal(t, keys[0], key1)
		require.Equal(t, keys[1], key2)
	}
	for range rand.Perm(10) { // Just running many iterations to cover multiple possible map iteration ordering. Perm() is just used as an array generator here.
		m := *NewInstanceKeyMap()
		m.AddKey(key2)
		m.AddKey(key1)
		keys := m.GetInstanceKeys()
		require.Equal(t, keys[0], key1)
		require.Equal(t, keys[1], key2)
	}
}

func TestInstanceKeyMapToJSON(t *testing.T) {
	m := *NewInstanceKeyMap()
	m.AddKey(key1)
	m.AddKey(key2)
	json, err := m.ToJSON()
	require.NoError(t, err)
	ok := (json == `[{"Hostname":"host1","Port":3306},{"Hostname":"host2","Port":3306}]`) || (json == `[{"Hostname":"host2","Port":3306},{"Hostname":"host1","Port":3306}]`)
	require.True(t, ok)
}

func TestInstanceKeyMapReadJSON(t *testing.T) {
	json := `[{"Hostname":"host1","Port":3306},{"Hostname":"host2","Port":3306}]`
	m := *NewInstanceKeyMap()
	_ = m.ReadJSON(json)
	require.Equal(t, len(m), 2)
	require.True(t, m[key1])
	require.True(t, m[key2])
}

func TestEmptyInstanceKeyMapToCommaDelimitedList(t *testing.T) {
	m := *NewInstanceKeyMap()
	res := m.ToCommaDelimitedList()

	require.Equal(t, res, "")
}

func TestInstanceKeyMapToCommaDelimitedList(t *testing.T) {
	m := *NewInstanceKeyMap()
	m.AddKey(key1)
	m.AddKey(key2)
	res := m.ToCommaDelimitedList()

	ok := (res == `host1:3306,host2:3306`) || (res == `host2:3306,host1:3306`)
	require.True(t, ok)
}

func TestIntersect(t *testing.T) {
	{
		m := NewInstanceKeyMap()
		m.AddKey(key1)
		m.AddKey(key2)

		other := NewInstanceKeyMap()
		other.AddKey(key3)
		other.AddKey(key2)

		intersected := m.Intersect(other)
		require.Equal(t, len(*intersected), 1)
	}
	{
		m := NewInstanceKeyMap()
		m.AddKey(key1)

		other := NewInstanceKeyMap()
		other.AddKey(key3)
		other.AddKey(key2)

		intersected := m.Intersect(other)
		require.Equal(t, len(*intersected), 0)
	}
	{
		m := NewInstanceKeyMap()
		m.AddKey(key1)
		m.AddKey(key2)

		other := NewInstanceKeyMap()
		other.AddKey(key1)
		other.AddKey(key3)
		other.AddKey(key2)

		intersected := m.Intersect(other)
		require.Equal(t, len(*intersected), 2)
	}

}
