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
