/*
Copyright 2021 The Vitess Authors.

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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersionString(t *testing.T) {
	now, _ := time.Parse(time.RFC1123, "Tue, 15 Sep 2020 12:04:10 UTC")

	v := &versionInfo{
		buildHost:       "host",
		buildUser:       "user",
		buildTime:       now.Unix(),
		buildTimePretty: "time is now",
		buildGitRev:     "d54b87ca0be09b678bb4490060e8f23f890ddb92",
		buildGitBranch:  "gitBranch",
		goVersion:       "1.20.2",
		goOS:            "amiga",
		goArch:          "amd64",
		version:         "v1.2.3-SNAPSHOT",
	}

	// Test case 1: No build number or system
	assert.Equal(t, "Version: v1.2.3-SNAPSHOT (Git revision d54b87ca0be09b678bb4490060e8f23f890ddb92 branch 'gitBranch') built on time is now by user@host using 1.20.2 amiga/amd64", v.String())

	// Test case 2: With build number but no build system
	v.buildNumber = 422
	assert.Equal(t, "Version: v1.2.3-SNAPSHOT (build 422) (Git revision d54b87ca0be09b678bb4490060e8f23f890ddb92 branch 'gitBranch') built on time is now by user@host using 1.20.2 amiga/amd64", v.String())

	// Test case 3: With build number and custom build system
	v.buildSystem = "GHA"
	assert.Equal(t, "Version: v1.2.3-SNAPSHOT (GHA build 422) (Git revision d54b87ca0be09b678bb4490060e8f23f890ddb92 branch 'gitBranch') built on time is now by user@host using 1.20.2 amiga/amd64", v.String())

	assert.Equal(t, "8.4.6-Vitess", v.MySQLVersion())
}

func TestResolveGitRev(t *testing.T) {
	// VCS revision present and clean: used verbatim, ldflag fallback ignored.
	assert.Equal(t, "abc123", resolveGitRev("abc123", false, "fallback"))

	// VCS revision present and dirty: "-dirty" suffix appended.
	assert.Equal(t, "abc123-dirty", resolveGitRev("abc123", true, "fallback"))

	// No VCS revision (e.g. tarball build): the ldflag fallback is used as-is,
	// regardless of the modified flag.
	assert.Equal(t, "fallback", resolveGitRev("", false, "fallback"))
	assert.Equal(t, "fallback", resolveGitRev("", true, "fallback"))

	// No VCS revision and no fallback: empty.
	assert.Empty(t, resolveGitRev("", false, ""))
}

func TestParseBuildTime(t *testing.T) {
	// No override and no commit time yields zero values.
	pretty, unix := parseBuildTime("", "")
	assert.Empty(t, pretty)
	assert.Equal(t, int64(0), unix)

	// RFC3339 commit time (as stamped by the Go toolchain) is parsed when no
	// override is set.
	const commitTime = "2020-09-15T12:04:10Z"
	expectedVCS, err := time.Parse(time.RFC3339, commitTime)
	require.NoError(t, err)
	pretty, unix = parseBuildTime("", commitTime)
	assert.Equal(t, commitTime, pretty)
	assert.Equal(t, expectedVCS.Unix(), unix)

	// An explicit BUILD_TIME override (UnixDate format, as emitted by `date`) is
	// used as-is. This covers package/tarball builds.
	const override = "Tue Sep 15 12:04:10 UTC 2020"
	expectedOverride, err := time.Parse(time.UnixDate, override)
	require.NoError(t, err)
	pretty, unix = parseBuildTime(override, "")
	assert.Equal(t, override, pretty)
	assert.Equal(t, expectedOverride.Unix(), unix)

	// The override takes precedence over the VCS commit time.
	pretty, unix = parseBuildTime(override, commitTime)
	assert.Equal(t, override, pretty)
	assert.Equal(t, expectedOverride.Unix(), unix)

	// An unparseable override is ignored and falls through to the VCS time.
	pretty, unix = parseBuildTime("not a date", commitTime)
	assert.Equal(t, commitTime, pretty)
	assert.Equal(t, expectedVCS.Unix(), unix)

	// An unparseable override with no VCS time yields zero values.
	pretty, unix = parseBuildTime("not a date", "")
	assert.Empty(t, pretty)
	assert.Equal(t, int64(0), unix)

	// A non-RFC3339 VCS time is unexpected from the toolchain and panics.
	assert.Panics(t, func() { parseBuildTime("", "Tue Sep 15 12:04:10 UTC 2020") })
}

func TestBuildVersionStats(t *testing.T) {
	buildVersion := statsBuildVersion.Get()
	assert.Equal(t, versionName, buildVersion)
}
