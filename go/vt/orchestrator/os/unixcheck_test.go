/*
   Copyright 2017 Simon Mudd, courtesy Booking.com

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
package os

import (
	"testing"
)

type testCase struct {
	user       string
	powerUsers []string
	expected   bool
}

var testCases []testCase

func init() {
	// It is hard to come up with good results that will work on all systems
	// so the tests are limited but should work on most Linux or OSX systems.
	// If you find a case where the tests fail due to user differences please
	// adjust the test cases appropriately.
	testCases = []testCase{
		{"root", []string{"root", "wheel"}, true},
		{"root", []string{"not_in_this_group"}, false},
		{"not_found_user", []string{"not_in_this_group"}, false},
	}
}

// test the users etc
func TestUsers(t *testing.T) {
	for _, v := range testCases {
		if got := UserInGroups(v.user, v.powerUsers); got != v.expected {
			t.Errorf("userInGroups(%q,%+v) failed. Got %v, Expected %v", v.user, v.powerUsers, got, v.expected)
		}
	}
}
