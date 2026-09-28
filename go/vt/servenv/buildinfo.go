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
	"log/slog"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
)

var (
	buildHost         = ""
	buildUser         = ""
	buildTimeOverride = ""
	buildGitRev       = ""
	buildGitBranch    = ""
	statsBuildVersion *stats.String
	buildNumberStr    = ""
	buildSystem       = ""

	// version registers the command line flag to expose build info.
	version bool
)

func registerVersionFlag(fs *pflag.FlagSet) {
	fs.BoolVarP(&version, "version", "v", version, "print binary version")
}

// AppVersion is the struct to store build info.
var AppVersion versionInfo

type versionInfo struct {
	buildHost       string
	buildUser       string
	buildTime       int64
	buildTimePretty string
	buildGitRev     string
	buildGitBranch  string
	buildNumber     int64
	buildSystem     string
	goVersion       string
	goOS            string
	goArch          string
	version         string
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
	buildInfo := ""
	if v.buildNumber != 0 {
		if v.buildSystem != "" {
			buildInfo = fmt.Sprintf(" (%s build %d)", v.buildSystem, v.buildNumber)
		} else {
			buildInfo = fmt.Sprintf(" (build %d)", v.buildNumber)
		}
	}
	return fmt.Sprintf("Version: %s%s (Git revision %s branch '%s') built on %s by %s@%s using %s %s/%s",
		v.version, buildInfo, v.buildGitRev, v.buildGitBranch, v.buildTimePretty, v.buildUser, v.buildHost, v.goVersion, v.goOS, v.goArch)
}

func (v *versionInfo) MySQLVersion() string {
	return mySQLServerVersion
}

// readVCSInfo returns the VCS revision, commit time, and dirty state that the Go
// toolchain stamps into the binary. The values are empty/false when the binary
// was built without VCS metadata (for example from a release tarball).
func readVCSInfo() (revision, revisionTime string, modified bool) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "", "", false
	}
	for _, setting := range bi.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.time":
			revisionTime = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	return revision, revisionTime, modified
}

// resolveGitRev prefers the VCS-stamped revision, falling back to the value
// injected via ldflags when no VCS metadata is available. A dirty working tree
// is reflected with a "-dirty" suffix.
func resolveGitRev(vcsRevision string, modified bool, fallback string) string {
	if vcsRevision == "" {
		return fallback
	}
	if modified {
		return vcsRevision + "-dirty"
	}
	return vcsRevision
}

// parseBuildTime resolves the build timestamp into the pretty string and Unix
// timestamp used by AppVersion. An explicitly injected BUILD_TIME (via ldflags,
// in time.UnixDate format) takes precedence so that package/tarball builds can set
// it deliberately; otherwise it uses the VCS-stamped commit time (RFC3339), which
// the Go toolchain records for normal git builds. An unparseable override is logged
// and ignored so it falls through to the VCS time. It returns zero values when no
// usable timestamp is available.
func parseBuildTime(override, vcsTime string) (pretty string, unix int64) {
	if override != "" {
		t, err := time.Parse(time.UnixDate, override)
		if err == nil {
			return override, t.Unix()
		}
		log.Warn("Ignoring unparseable BUILD_TIME override", slog.String("build_time", override), slog.Any("error", err))
	}
	if vcsTime != "" {
		t, err := time.Parse(time.RFC3339, vcsTime)
		if err != nil {
			panic(fmt.Sprintf("Couldn't parse build timestamp %q: %v", vcsTime, err))
		}
		return vcsTime, t.Unix()
	}
	return "", 0
}

func init() {
	revision, revisionTime, modified := readVCSInfo()
	gitRev := resolveGitRev(revision, modified, buildGitRev)
	buildTimePretty, buildTime := parseBuildTime(buildTimeOverride, revisionTime)

	buildNumber, err := strconv.ParseInt(buildNumberStr, 10, 64)
	if err != nil {
		buildNumber = 0
	}

	AppVersion = versionInfo{
		buildHost:       buildHost,
		buildUser:       buildUser,
		buildTime:       buildTime,
		buildTimePretty: buildTimePretty,
		buildGitRev:     gitRev,
		buildGitBranch:  buildGitBranch,
		buildNumber:     buildNumber,
		buildSystem:     buildSystem,
		goVersion:       runtime.Version(),
		goOS:            runtime.GOOS,
		goArch:          runtime.GOARCH,
		version:         versionName,
	}
	stats.NewString("BuildHost").Set(AppVersion.buildHost)
	stats.NewString("BuildUser").Set(AppVersion.buildUser)
	stats.NewGauge("BuildTimestamp", "build timestamp").Set(AppVersion.buildTime)
	statsBuildVersion = stats.NewString("BuildVersion")
	statsBuildVersion.Set(AppVersion.version)
	stats.NewString("BuildGitRev").Set(AppVersion.buildGitRev)
	stats.NewString("BuildGitBranch").Set(AppVersion.buildGitBranch)
	stats.NewGauge("BuildNumber", "build number").Set(AppVersion.buildNumber)
	stats.NewString("GoVersion").Set(AppVersion.goVersion)
	stats.NewString("GoOS").Set(AppVersion.goOS)
	stats.NewString("GoArch").Set(AppVersion.goArch)

	buildLabels := []string{"BuildHost", "BuildUser", "BuildTimestamp", "BuildGitRev", "BuildGitBranch", "BuildNumber"}
	buildValues := []string{
		AppVersion.buildHost,
		AppVersion.buildUser,
		strconv.FormatInt(AppVersion.buildTime, 10),
		AppVersion.buildGitRev,
		AppVersion.buildGitBranch,
		strconv.FormatInt(AppVersion.buildNumber, 10),
	}
	stats.NewGaugesWithMultiLabels("BuildInformation", "build information exposed via label", buildLabels).Set(buildValues, 1)

	OnParse(registerVersionFlag)
}
