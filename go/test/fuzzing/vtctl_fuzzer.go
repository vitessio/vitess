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
// +build gofuzz

package fuzzing

import (
	"context"
	"strings"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"
)

func init() {
	*tmclient.TabletManagerProtocol = "fuzzing"
	tmclient.RegisterTabletManagerClientFactory("fuzzing", func() tmclient.TabletManagerClient {
		return nil
	})
}

func IsDivisibleBy(n int, divisibleby int) bool {
	return (n % divisibleby) == 0
}

func getCommandType(index int) string {

	m := map[int]string{
		0:  "GetTablet", // Tablets
		1:  "InitTablet",
		2:  "UpdateTabletAddrs",
		3:  "DeleteTablet",
		4:  "SetReadOnly",
		5:  "SetReadWrite",
		6:  "StartReplication",
		7:  "StopReplication",
		8:  "ChangeTabletType",
		9:  "Ping",
		10: "RefreshState",
		11: "RefreshStateByShard",
		12: "RunHealthCheck",
		13: "IgnoreHealthCheck",
		14: "IgnoreHealthError",
		15: "ExecuteHook",
		16: "ExecuteFetchAsApp",
		17: "ExecuteFetchAsDba",
		18: "VReplicationExec",
		19: "Backup",
		20: "RestoreFromBackup",
		21: "ReparentTablet",
		22: "CreateShard", // Shards
		23: "GetShard",
		24: "ValidateShard",
		25: "ShardReplicationPositions",
		26: "ListShardTablets",
		27: "SetShardIsMasterServing",
		28: "SetShardTabletControl",
		29: "UpdateSrvKeyspacePartition",
		30: "SourceShardDelete",
		31: "SourceShardAdd",
		32: "ShardReplicationFix",
		33: "WaitForFilteredReplication",
		34: "RemoveShardCell",
		35: "DeleteShard",
		36: "ListBackups",
		37: "BackupShard",
		38: "RemoveBackup",
		39: "InitShardMaster",
		40: "PlannedReparentShard",
		41: "EmergencyReparentShard",
		42: "TabletExternallyReparented",
		43: "CreateKeyspace", // Keyspaces
		44: "DeleteKeyspace",
		45: "RemoveKeyspaceCell",
		46: "GetKeyspace",
		47: "GetKeyspaces",
		48: "SetKeyspaceShardingInfo",
		49: "SetKeyspaceServedFrom",
		50: "RebuildKeyspaceGraph",
		51: "ValidateKeyspace",
		52: "Reshard",
		53: "MoveTables",
		54: "DropSources",
		55: "CreateLookupVindex",
		56: "ExternalizeVindex",
		57: "Materialize",
		58: "SplitClone",
		59: "VerticalSplitClone",
		60: "VDiff",
		61: "MigrateServedTypes",
		62: "MigrateServedFrom",
		63: "SwitchReads",
		64: "SwitchWrites",
		65: "CancelResharding",
		66: "ShowResharding",
		67: "FindAllShardsInKeyspace",
		68: "WaitForDrain",
	}
	return m[index]

}

/*
	In this fuzzer we split the input into 3 chunks:
	1: the first byte - Is converted to an int, and
	   that int determines the number of command-line
	   calls the fuzzer will make.
	2: The next n bytes where n is equal to the int from
	   the first byte. These n bytes are converted to
	   a corresponding command and represent which
	   commands will be called.
	3: The rest of the data array should have a length
	   that is divisible by the number of calls.
	   This part is split up into equally large chunks,
	   and each chunk is used as parameters for the
	   corresponding command.
*/
func Fuzz(data []byte) int {

	//  Basic checks
	if len(data) == 0 {
		return -1
	}
	numberOfCalls := int(data[0])
	if numberOfCalls < 3 || numberOfCalls > 10 {
		return -1
	}
	if len(data) < numberOfCalls+numberOfCalls+1 {
		return -1
	}

	// Define part 2 and 3 of the data array
	commandPart := data[1 : numberOfCalls+1]
	restOfArray := data[numberOfCalls+1:]

	// Just a small check. It is necessary
	if len(commandPart) != numberOfCalls {
		return -1
	}

	// Check if restOfArray is divisible by numberOfCalls
	if !IsDivisibleBy(len(restOfArray), numberOfCalls) {
		return -1
	}

	// At this point we have a data array that can
	// be divided properly. We can now proceed to
	// passing it to Vitess
	ctx := context.Background()
	topo, err := createTopo(ctx)
	if err != nil {
		return -1
	}
	tmc := tmclient.NewTabletManagerClient()
	logger := logutil.NewMemoryLogger()

	chunkSize := len(restOfArray) / numberOfCalls
	command := 0
	for i := 0; i < len(restOfArray); i = i + chunkSize {
		from := i           //lower
		to := i + chunkSize //upper

		// Index of command in getCommandType():
		commandIndex := int(commandPart[command]) % 68
		vtCommand := getCommandType(commandIndex)
		command_slice := []string{vtCommand}
		args := strings.Split(string(restOfArray[from:to]), " ")

		// Add params to the command
		for i := range args {
			command_slice = append(command_slice, args[i])
		}

		_ = vtctl.RunCommand(ctx, wrangler.New(logger, topo, tmc), command_slice)
		command++
	}

	return 1

}

func createTopo(ctx context.Context) (*topo.Server, error) {
	ts := memorytopo.NewServer("zone1", "zone2", "zone3")
	return ts, nil
}
