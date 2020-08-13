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
	"os/user"
	"strings"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
)

// UserInGroups checks if the given username is in the given unix
// groups.  It might be worth caching this for performance reasons.
func UserInGroups(authUser string, powerAuthGroups []string) bool {
	// these conditions are treated as false
	if authUser == "" || len(powerAuthGroups) == 0 {
		return false
	}

	// make a map (likely to have only one group maybe) of power groups
	powerGroupMap := make(map[string]bool)
	for _, v := range powerAuthGroups {
		powerGroupMap[v] = true
	}

	currentUser, err := user.Lookup(authUser)
	if err != nil {
		// The user not being known is not an error so don't report this.
		// ERROR Failed to lookup user "simon": user: unknown user simon
		if !strings.Contains(err.Error(), "unknown user") {
			log.Errorf("Failed to lookup user %q: %v", authUser, err)
		}
		return false
	}
	gids, err := currentUser.GroupIds()
	if err != nil {
		log.Errorf("Failed to lookup groupids for user %q: %v", authUser, err)
		return false
	}
	// get the group name from the id and check if the name is in powerGroupMap
	for _, gid := range gids {
		group, err := user.LookupGroupId(gid)
		if err != nil {
			log.Errorf("Failed to lookup group id for gid %s: %v", gid, err) // yes gids are strings!
			return false
		}

		if _, found := powerGroupMap[group.Name]; found {
			return true
		}
	}

	return false
}
