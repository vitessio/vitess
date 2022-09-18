/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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

package orcraft

import (
	"fmt"
)

var RaftNotRunning error = fmt.Errorf("raft is not configured/running")

// PublishCommand will distribute a command across the group
func PublishCommand(op string, value any) (response any, err error) {
	return nil, RaftNotRunning
}

func IsRaftEnabled() bool {
	return false
}
