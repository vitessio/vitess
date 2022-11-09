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
		goVersion:       "1.19.3",
		goOS:            "amiga",
		goArch:          "amd64",
		version:         "v1.2.3-SNAPSHOT",
	}

	assert.Equal(t, "Version: v1.2.3-SNAPSHOT (Git revision d54b87ca0be09b678bb4490060e8f23f890ddb92 branch 'gitBranch') built on time is now by user@host using 1.19.3 amiga/amd64", v.String())

	v.jenkinsBuildNumber = 422

	assert.Equal(t, "Version: v1.2.3-SNAPSHOT (Jenkins build 422) (Git revision d54b87ca0be09b678bb4490060e8f23f890ddb92 branch 'gitBranch') built on time is now by user@host using 1.19.3 amiga/amd64", v.String())

	assert.Equal(t, "5.7.9-vitess-v1.2.3-SNAPSHOT", v.MySQLVersion())
}
