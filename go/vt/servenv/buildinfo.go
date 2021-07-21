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
	"flag"
	"fmt"
	"runtime"
	"strconv"
	"time"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/stats"
)

var (
	// MySQLServerVersion is what Vitess will present as it's version during the connection handshake,
	// and as the value to the @@version system variable. If nothing is provided, Vitess will report itself as
	// a specific MySQL version with the vitess version appended to it
	MySQLServerVersion = flag.String("mysql_server_version", "", "MySQL server version to advertise.")

	buildHost             = ""
	buildUser             = ""
	buildTime             = ""
	buildGitRev           = ""
	buildGitBranch        = ""
	jenkinsBuildNumberStr = ""

	// Version registers the command line flag to expose build info.
	Version = flag.Bool("version", false, "print binary version")
)

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
	if *MySQLServerVersion != "" {
		return *MySQLServerVersion
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
	var convVersion string
	convVersion, err = convertMySQLVersionToCommentVersion(AppVersion.MySQLVersion())
	if err != nil {
		log.Error(err)
	} else {
		sqlparser.MySQLVersion = convVersion
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
}

// convertMySQLVersionToCommentVersion converts the MySQL version into comment version format.
func convertMySQLVersionToCommentVersion(version string) (string, error) {
	var res = make([]int, 3)
	idx := 0
	val := ""
	for _, c := range version {
		if c <= '9' && c >= '0' {
			val += string(c)
		} else if c == '.' {
			v, err := strconv.Atoi(val)
			if err != nil {
				return "", err
			}
			val = ""
			res[idx] = v
			idx++
			if idx == 3 {
				break
			}
		} else {
			break
		}
	}
	if val != "" {
		v, err := strconv.Atoi(val)
		if err != nil {
			return "", err
		}
		res[idx] = v
		idx++
	}
	if idx == 0 {
		return "", vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "MySQL version not correctly setup - %s.", version)
	}

	return fmt.Sprintf("%01d%02d%02d", res[0], res[1], res[2]), nil
}
