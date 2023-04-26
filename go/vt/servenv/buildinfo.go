/*
Copyright 2019 The Vitess Authors.

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

package servenv

import (
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/stats"
)

var (
	buildHost             = ""
	buildUser             = ""
	buildTime             = ""
	buildGitRev           = ""
	buildGitBranch        = ""
	jenkinsBuildNumberStr = ""

	// version registers the command line flag to expose build info.
	version bool
)

func registerVersionFlag(fs *pflag.FlagSet) {
	fs.BoolVarP(&version, "version", "v", version, "print binary version")
}

// AppVersion is the struct to store build info.
var AppVersion versionInfo

type versionInfo struct {
	buildHost          string
	buildUser          string
	buildTime          int64
	buildTimePretty    string
	buildGitRev        string
	buildGitBranch     string
	jenkinsBuildNumber int64
	goVersion          string
	goOS               string
	goArch             string
	version            string
}

// ToStringMap returns the version info as a map[string]string, allowing version
// info to be used in things like arbitrary string-tag maps (e.g. tablet tags
// in the topo).
func (v *versionInfo) ToStringMap() map[string]string {
	return map[string]string{
		"build_host":       v.buildHost,
		"build_user":       v.buildUser,
		"build_time":       v.buildTimePretty,
		"build_git_rev":    v.buildGitRev,
		"build_git_branch": v.buildGitBranch,
		"go_version":       v.goVersion,
		"goos":             v.goOS,
		"goarch":           v.goArch,
		"version":          v.version,
	}
}

func (v *versionInfo) Print() {
	fmt.Println(v)
}

func (v *versionInfo) String() string {
	jenkins := ""
	if v.jenkinsBuildNumber != 0 {
		jenkins = fmt.Sprintf(" (Jenkins build %d)", v.jenkinsBuildNumber)
	}
	return fmt.Sprintf("Version: %s%s (Git revision %s branch '%s') built on %s by %s@%s using %s %s/%s",
		v.version, jenkins, v.buildGitRev, v.buildGitBranch, v.buildTimePretty, v.buildUser, v.buildHost, v.goVersion, v.goOS, v.goArch)
}

func (v *versionInfo) MySQLVersion() string {
	if mySQLServerVersion != "" {
		return mySQLServerVersion
	}
	return "5.7.9-vitess-" + v.version
}

func init() {
	t, err := time.Parse(time.UnixDate, buildTime)
	if buildTime != "" && err != nil {
		panic(fmt.Sprintf("Couldn't parse build timestamp %q: %v", buildTime, err))
	}

	jenkinsBuildNumber, err := strconv.ParseInt(jenkinsBuildNumberStr, 10, 64)
	if err != nil {
		jenkinsBuildNumber = 0
	}

	AppVersion = versionInfo{
		buildHost:          buildHost,
		buildUser:          buildUser,
		buildTime:          t.Unix(),
		buildTimePretty:    buildTime,
		buildGitRev:        buildGitRev,
		buildGitBranch:     buildGitBranch,
		jenkinsBuildNumber: jenkinsBuildNumber,
		goVersion:          runtime.Version(),
		goOS:               runtime.GOOS,
		goArch:             runtime.GOARCH,
		version:            versionName,
	}
	stats.NewString("BuildHost").Set(AppVersion.buildHost)
	stats.NewString("BuildUser").Set(AppVersion.buildUser)
	stats.NewGauge("BuildTimestamp", "build timestamp").Set(AppVersion.buildTime)
	stats.NewString("BuildGitRev").Set(AppVersion.buildGitRev)
	stats.NewString("BuildGitBranch").Set(AppVersion.buildGitBranch)
	stats.NewGauge("BuildNumber", "build number").Set(AppVersion.jenkinsBuildNumber)
	stats.NewString("GoVersion").Set(AppVersion.goVersion)
	stats.NewString("GoOS").Set(AppVersion.goOS)
	stats.NewString("GoArch").Set(AppVersion.goArch)

	buildLabels := []string{"BuildHost", "BuildUser", "BuildTimestamp", "BuildGitRev", "BuildGitBranch", "BuildNumber"}
	buildValues := []string{
		AppVersion.buildHost,
		AppVersion.buildUser,
		fmt.Sprintf("%v", AppVersion.buildTime),
		AppVersion.buildGitRev,
		AppVersion.buildGitBranch,
		fmt.Sprintf("%v", AppVersion.jenkinsBuildNumber),
	}
	stats.NewGaugesWithMultiLabels("BuildInformation", "build information exposed via label", buildLabels).Set(buildValues, 1)

	OnParse(registerVersionFlag)
}
