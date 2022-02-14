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
		buildGitRev:     "d54b87c",
		buildGitBranch:  "gitBranch",
		goVersion:       "1.17",
		goOS:            "amiga",
		goArch:          "amd64",
		version:         "v1.2.3-SNAPSHOT",
	}

	assert.Equal(t, "Version: v1.2.3-SNAPSHOT (Git revision d54b87c branch 'gitBranch') built on time is now by user@host using 1.17 amiga/amd64", v.String())

	v.jenkinsBuildNumber = 422

	assert.Equal(t, "Version: v1.2.3-SNAPSHOT (Jenkins build 422) (Git revision d54b87c branch 'gitBranch') built on time is now by user@host using 1.17 amiga/amd64", v.String())

	assert.Equal(t, "5.7.9-vitess-v1.2.3-SNAPSHOT", v.MySQLVersion())
}
