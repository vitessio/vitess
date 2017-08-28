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
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/youtube/vitess/go/stats"
)

var (
	buildHost      = ""
	buildUser      = ""
	buildTime      = ""
	buildGitRev    = ""
	buildGitBranch = ""

	// Version registers the command line flag to expose build info.
	Version = flag.Bool("version", false, "print binary version")
)

// AppVersion is the struct to store build info.
var AppVersion versionInfo

type versionInfo struct {
	buildHost       string
	buildUser       string
	buildTime       int64
	buildTimePretty string
	buildGitRev     string
	buildGitBranch  string
	goVersion       string
	goOS            string
	goArch          string
}

func (v *versionInfo) Print() {
	fmt.Printf("Version: %s (Git branch '%s') built on %s by %s@%s using %s %s/%s\n", v.buildGitRev, v.buildGitBranch, v.buildTimePretty, v.buildUser, v.buildHost, v.goVersion, v.goOS, v.goArch)
}

func init() {
	t, err := time.Parse(time.UnixDate, buildTime)
	if buildTime != "" && err != nil {
		panic(fmt.Sprintf("Couldn't parse build timestamp %q: %v", buildTime, err))
	}

	AppVersion = versionInfo{
		buildHost:       buildHost,
		buildUser:       buildUser,
		buildTime:       t.Unix(),
		buildTimePretty: buildTime,
		buildGitRev:     buildGitRev,
		buildGitBranch:  buildGitBranch,
		goVersion:       runtime.Version(),
		goOS:            runtime.GOOS,
		goArch:          runtime.GOARCH,
	}

	stats.NewString("BuildHost").Set(AppVersion.buildHost)
	stats.NewString("BuildUser").Set(AppVersion.buildUser)
	stats.NewInt("BuildTimestamp").Set(AppVersion.buildTime)
	stats.NewString("BuildGitRev").Set(AppVersion.buildGitRev)
	stats.NewString("BuildGitBranch").Set(AppVersion.buildGitBranch)
	stats.NewString("GoVersion").Set(AppVersion.goVersion)
	stats.NewString("GoOS").Set(AppVersion.goOS)
	stats.NewString("GoArch").Set(AppVersion.goArch)

}
