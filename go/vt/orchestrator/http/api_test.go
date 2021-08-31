package http

import (
	"strings"
	"testing"

	"github.com/go-martini/martini"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	test "vitess.io/vitess/go/vt/orchestrator/external/golib/tests"
)

func init() {
	config.Config.HostnameResolveMethod = "none"
	config.MarkConfigurationLoaded()
	log.SetLevel(log.ERROR)
}

func TestKnownPaths(t *testing.T) {
	m := martini.Classic()
	api := HttpAPI{}

	api.RegisterRequests(m)

	pathsMap := make(map[string]bool)
	for _, path := range registeredPaths {
		pathBase := strings.Split(path, "/")[0]
		pathsMap[pathBase] = true
	}
	test.S(t).ExpectTrue(pathsMap["health"])
	test.S(t).ExpectTrue(pathsMap["lb-check"])
	test.S(t).ExpectTrue(pathsMap["relocate"])
	test.S(t).ExpectTrue(pathsMap["relocate-replicas"])
}
