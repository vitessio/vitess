/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package servenv

import (
	"fmt"
	"time"

	"github.com/youtube/vitess/go/stats"
)

var (
	buildHost   = ""
	buildUser   = ""
	buildTime   = ""
	buildGitRev = ""
)

func init() {
	t, err := time.Parse(time.UnixDate, buildTime)
	if buildTime != "" && err != nil {
		panic(fmt.Sprintf("Couldn't parse build timestamp %q: %v", buildTime, err))
	}
	stats.NewString("BuildHost").Set(buildHost)
	stats.NewString("BuildUser").Set(buildUser)
	stats.NewInt("BuildTimestamp").Set(t.Unix())
	stats.NewString("BuildGitRev").Set(buildGitRev)
}
