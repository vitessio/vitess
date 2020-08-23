package app

import (
	"testing"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	test "vitess.io/vitess/go/vt/orchestrator/external/golib/tests"
)

func init() {
	config.Config.HostnameResolveMethod = "none"
	config.MarkConfigurationLoaded()
	log.SetLevel(log.ERROR)
}

func TestHelp(t *testing.T) {
	Cli("help", false, "localhost:9999", "localhost:9999", "orc", "no-reason", "1m", ".", "no-alias", "no-pool", "")
	test.S(t).ExpectTrue(len(knownCommands) > 0)
}

func TestKnownCommands(t *testing.T) {
	Cli("help", false, "localhost:9999", "localhost:9999", "orc", "no-reason", "1m", ".", "no-alias", "no-pool", "")

	commandsMap := make(map[string]string)
	for _, command := range knownCommands {
		commandsMap[command.Command] = command.Section
	}
	test.S(t).ExpectEquals(commandsMap["no-such-command"], "")
	test.S(t).ExpectEquals(commandsMap["relocate"], "Smart relocation")
	test.S(t).ExpectEquals(commandsMap["relocate-slaves"], "")
	test.S(t).ExpectEquals(commandsMap["relocate-replicas"], "Smart relocation")

	for _, synonym := range commandSynonyms {
		test.S(t).ExpectNotEquals(commandsMap[synonym], "")
	}
}
