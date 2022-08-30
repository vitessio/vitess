/*
   Copyright 2014 Outbrain Inc.

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

package app

import (
	"fmt"
	"net"
	"os"
	"os/user"
	"sort"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/util"
	"vitess.io/vitess/go/vt/orchestrator/inst"
	"vitess.io/vitess/go/vt/orchestrator/logic"
	"vitess.io/vitess/go/vt/orchestrator/process"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
)

var thisInstanceKey *inst.InstanceKey
var knownCommands []CliCommand

type CliCommand struct {
	Command     string
	Section     string
	Description string
}

type stringSlice []string

func (a stringSlice) Len() int           { return len(a) }
func (a stringSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a stringSlice) Less(i, j int) bool { return a[i] < a[j] }

var commandSynonyms = map[string]string{
	"detach-replica":   "detach-replica-primary-host",
	"reattach-replica": "reattach-replica-primary-host",
}

func registerCliCommand(command string, section string, description string) string {
	if synonym, ok := commandSynonyms[command]; ok {
		command = synonym
	}
	knownCommands = append(knownCommands, CliCommand{Command: command, Section: section, Description: description})

	return command
}

func commandsListing() string {
	listing := []string{}
	lastSection := ""
	for _, cliCommand := range knownCommands {
		if lastSection != cliCommand.Section {
			lastSection = cliCommand.Section
			listing = append(listing, fmt.Sprintf("%s:", cliCommand.Section))
		}
		commandListing := fmt.Sprintf("\t%-40s%s", cliCommand.Command, cliCommand.Description)
		listing = append(listing, commandListing)
	}
	return strings.Join(listing, "\n")
}

func availableCommandsUsage() string {
	return fmt.Sprintf(`Available commands (-c):
%+v
Run 'orchestrator help <command>' for detailed help on given command, e.g. 'orchestrator help relocate'

Usage for most commands:
	orchestrator -c <command> [-i <instance.fqdn>[,<instance.fqdn>]* ] [-d <destination.fqdn>] [--verbose|--debug]
`, commandsListing())
}

// getClusterName will make a best effort to deduce a cluster name using either a given alias
// or an instanceKey. First attempt is at alias, and if that doesn't work, we try instanceKey.
func getClusterName(clusterAlias string, instanceKey *inst.InstanceKey) (clusterName string) {
	clusterName, _ = inst.FigureClusterName(clusterAlias, instanceKey, thisInstanceKey)
	return clusterName
}

func validateInstanceIsFound(instanceKey *inst.InstanceKey) (instance *inst.Instance) {
	instance, _, err := inst.ReadInstance(instanceKey)
	if err != nil {
		log.Fatal(err)
	}
	if instance == nil {
		log.Fatalf("Instance not found: %+v", *instanceKey)
	}
	return instance
}

// Cli initiates a command line interface, executing requested command.
func Cli(command string, strict bool, instance string, destination string, owner string, reason string, duration string, pattern string, clusterAlias string, pool string, hostnameFlag string) {
	if synonym, ok := commandSynonyms[command]; ok {
		command = synonym
	}

	skipDatabaseCommands := false
	switch command {
	case "redeploy-internal-db":
		skipDatabaseCommands = true
	case "help":
		skipDatabaseCommands = true
	case "dump-config":
		skipDatabaseCommands = true
	}

	instanceKey, err := inst.ParseResolveInstanceKey(instance)
	if err != nil {
		instanceKey = nil
	}

	rawInstanceKey, err := inst.ParseRawInstanceKey(instance)
	if err != nil {
		rawInstanceKey = nil
	}

	if destination != "" && !strings.Contains(destination, ":") {
		destination = fmt.Sprintf("%s:%d", destination, config.Config.DefaultInstancePort)
	}
	destinationKey, err := inst.ParseResolveInstanceKey(destination)
	if err != nil {
		destinationKey = nil
	}
	if !skipDatabaseCommands {
		destinationKey = inst.ReadFuzzyInstanceKeyIfPossible(destinationKey)
	}
	if hostname, err := os.Hostname(); err == nil {
		thisInstanceKey = &inst.InstanceKey{Hostname: hostname, Port: int(config.Config.DefaultInstancePort)}
	}
	postponedFunctionsContainer := inst.NewPostponedFunctionsContainer()

	if len(owner) == 0 {
		// get os username as owner
		usr, err := user.Current()
		if err != nil {
			log.Fatal(err)
		}
		owner = usr.Username
	}
	inst.SetMaintenanceOwner(owner)

	if !skipDatabaseCommands && !*config.RuntimeCLIFlags.SkipContinuousRegistration {
		process.ContinuousRegistration(string(process.OrchestratorExecutionCliMode), command)
	}

	// begin commands
	switch command {
	// smart mode
	case registerCliCommand("relocate", "Smart relocation", `Relocate a replica beneath another instance`), registerCliCommand("relocate-below", "Smart relocation", `Synonym to 'relocate', will be deprecated`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination:", destination)
			}
			_, err := inst.RelocateBelow(instanceKey, destinationKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%s<%s\n", instanceKey.DisplayString(), destinationKey.DisplayString())
		}
	case registerCliCommand("relocate-replicas", "Smart relocation", `Relocates all or part of the replicas of a given instance under another instance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination:", destination)
			}
			replicas, _, errs, err := inst.RelocateReplicas(instanceKey, destinationKey, pattern)
			if err != nil {
				log.Fatal(err)
			} else {
				for _, e := range errs {
					log.Error(e)
				}
				for _, replica := range replicas {
					fmt.Println(replica.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("take-siblings", "Smart relocation", `Turn all siblings of a replica into its sub-replicas.`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			_, _, err := inst.TakeSiblings(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("regroup-replicas", "Smart relocation", `Given an instance, pick one of its replicas and make it local primary of its siblings`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			validateInstanceIsFound(instanceKey)

			lostReplicas, equalReplicas, aheadReplicas, cannotReplicateReplicas, promotedReplica, err := inst.RegroupReplicas(instanceKey, false, func(candidateReplica *inst.Instance) { fmt.Println(candidateReplica.Key.DisplayString()) }, postponedFunctionsContainer)
			lostReplicas = append(lostReplicas, cannotReplicateReplicas...)

			postponedFunctionsContainer.Wait()
			if err != nil {
				log.Fatal(err)
			}
			if promotedReplica == nil { //nolint
				log.Fatalf("Could not regroup replicas of %+v; error: %+v", *instanceKey, err)
			}
			fmt.Printf("%s lost: %d, trivial: %d, pseudo-gtid: %d\n",
				promotedReplica.Key.DisplayString(), len(lostReplicas), len(equalReplicas), len(aheadReplicas)) //nolint
		}
		// General replication commands
		// move, binlog file:pos
	case registerCliCommand("move-up", "Classic file:pos relocation", `Move a replica one level up the topology`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			instance, err := inst.MoveUp(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%s<%s\n", instanceKey.DisplayString(), instance.SourceKey.DisplayString())
		}
	case registerCliCommand("move-up-replicas", "Classic file:pos relocation", `Moves replicas of the given instance one level up the topology`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}

			movedReplicas, _, errs, err := inst.MoveUpReplicas(instanceKey, pattern)
			if err != nil {
				log.Fatal(err)
			} else {
				for _, e := range errs {
					log.Error(e)
				}
				for _, replica := range movedReplicas {
					fmt.Println(replica.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("move-below", "Classic file:pos relocation", `Moves a replica beneath its sibling. Both replicas must be actively replicating from same primary.`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination/sibling:", destination)
			}
			_, err := inst.MoveBelow(instanceKey, destinationKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%s<%s\n", instanceKey.DisplayString(), destinationKey.DisplayString())
		}
	case registerCliCommand("repoint", "Classic file:pos relocation", `Make the given instance replicate from another instance without changing the binglog coordinates. Use with care`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			// destinationKey can be null, in which case the instance repoints to its existing primary
			instance, err := inst.Repoint(instanceKey, destinationKey, inst.GTIDHintNeutral)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%s<%s\n", instanceKey.DisplayString(), instance.SourceKey.DisplayString())
		}
	case registerCliCommand("repoint-replicas", "Classic file:pos relocation", `Repoint all replicas of given instance to replicate back from the instance. Use with care`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			repointedReplicas, errs, err := inst.RepointReplicasTo(instanceKey, pattern, destinationKey)
			if err != nil {
				log.Fatal(err)
			} else {
				for _, e := range errs {
					log.Error(e)
				}
				for _, replica := range repointedReplicas {
					fmt.Printf("%s<%s\n", replica.Key.DisplayString(), instanceKey.DisplayString())
				}
			}
		}
	case registerCliCommand("take-primary", "Classic file:pos relocation", `Turn an instance into a primary of its own primary; essentially switch the two.`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			_, err := inst.TakePrimary(instanceKey, false)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("make-co-primary", "Classic file:pos relocation", `Create a primary-primary replication. Given instance is a replica which replicates directly from a primary.`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := inst.MakeCoPrimary(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("get-candidate-replica", "Classic file:pos relocation", `Information command suggesting the most up-to-date replica of a given instance that is good for promotion`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}

			instance, _, _, _, _, err := inst.GetCandidateReplica(instanceKey, false)
			if err != nil {
				log.Fatal(err)
			} else {
				fmt.Println(instance.Key.DisplayString())
			}
		}
	case registerCliCommand("regroup-replicas-bls", "Binlog server relocation", `Regroup Binlog Server replicas of a given instance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			validateInstanceIsFound(instanceKey)

			_, promotedBinlogServer, err := inst.RegroupReplicasBinlogServers(instanceKey, false)
			if err != nil {
				log.Fatal(err)
			}
			if promotedBinlogServer == nil { //nolint
				log.Fatalf("Could not regroup binlog server replicas of %+v; error: %+v", *instanceKey, err)
			}
			fmt.Println(promotedBinlogServer.Key.DisplayString()) //nolint
		}
	// move, GTID
	case registerCliCommand("move-gtid", "GTID relocation", `Move a replica beneath another instance.`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination:", destination)
			}
			_, err := inst.MoveBelowGTID(instanceKey, destinationKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%s<%s\n", instanceKey.DisplayString(), destinationKey.DisplayString())
		}
	case registerCliCommand("move-replicas-gtid", "GTID relocation", `Moves all replicas of a given instance under another (destination) instance using GTID`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination:", destination)
			}
			movedReplicas, _, errs, err := inst.MoveReplicasGTID(instanceKey, destinationKey, pattern)
			if err != nil {
				log.Fatal(err)
			} else {
				for _, e := range errs {
					log.Error(e)
				}
				for _, replica := range movedReplicas {
					fmt.Println(replica.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("regroup-replicas-gtid", "GTID relocation", `Given an instance, pick one of its replica and make it local primary of its siblings, using GTID.`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			validateInstanceIsFound(instanceKey)

			lostReplicas, movedReplicas, cannotReplicateReplicas, promotedReplica, err := inst.RegroupReplicasGTID(instanceKey, false, func(candidateReplica *inst.Instance) { fmt.Println(candidateReplica.Key.DisplayString()) }, postponedFunctionsContainer, nil)
			lostReplicas = append(lostReplicas, cannotReplicateReplicas...)

			if err != nil {
				log.Fatal(err)
			}
			if promotedReplica == nil { //nolint
				log.Fatalf("Could not regroup replicas of %+v; error: %+v", *instanceKey, err)
			}
			fmt.Printf("%s lost: %d, moved: %d\n", promotedReplica.Key.DisplayString(), len(lostReplicas), len(movedReplicas)) //nolint
		}
		// General replication commands
	case registerCliCommand("enable-gtid", "Replication, general", `If possible, turn on GTID replication`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := inst.EnableGTID(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("disable-gtid", "Replication, general", `Turn off GTID replication, back to file:pos replication`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := inst.DisableGTID(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("which-gtid-errant", "Replication, general", `Get errant GTID set (empty results if no errant GTID)`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)

			instance, err := inst.ReadTopologyInstance(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			if instance == nil { //nolint
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			fmt.Println(instance.GtidErrant) //nolint
		}
	case registerCliCommand("gtid-errant-reset-primary", "Replication, general", `Reset primary on instance, remove GTID errant transactions`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := inst.ErrantGTIDResetPrimary(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("skip-query", "Replication, general", `Skip a single statement on a replica; either when running with GTID or without`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := inst.SkipQuery(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("stop-replica", "Replication, general", `Issue a STOP SLAVE on an instance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := inst.StopReplication(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("start-replica", "Replication, general", `Issue a START SLAVE on an instance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := inst.StartReplication(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("restart-replica", "Replication, general", `STOP and START SLAVE on an instance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := inst.RestartReplication(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("reset-replica", "Replication, general", `Issues a RESET SLAVE command; use with care`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := inst.ResetReplicationOperation(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("detach-replica-primary-host", "Replication, general", `Stops replication and modifies Master_Host into an impossible, yet reversible, value.`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			_, err := inst.DetachReplicaPrimaryHost(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("reattach-replica-primary-host", "Replication, general", `Undo a detach-replica-primary-host operation`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}
			_, err := inst.ReattachReplicaPrimaryHost(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("primary-pos-wait", "Replication, general", `Wait until replica reaches given replication coordinates (--binlog=file:pos)`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance, err := inst.ReadTopologyInstance(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			if instance == nil {
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			var binlogCoordinates *inst.BinlogCoordinates

			if binlogCoordinates, err = inst.ParseBinlogCoordinates(*config.RuntimeCLIFlags.BinlogFile); err != nil {
				log.Fatalf("Expecing --binlog argument as file:pos")
			}
			_, err = inst.PrimaryPosWait(instanceKey, binlogCoordinates)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("restart-replica-statements", "Replication, general", `Get a list of statements to execute to stop then restore replica to same execution state. Provide --statement for injected statement`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			statements, err := inst.GetReplicationRestartPreserveStatements(instanceKey, *config.RuntimeCLIFlags.Statement)
			if err != nil {
				log.Fatal(err)
			}
			for _, statement := range statements {
				fmt.Println(statement)
			}
		}
		// Replication, information
	case registerCliCommand("can-replicate-from", "Replication information", `Can an instance (-i) replicate from another (-d) according to replication rules? Prints 'true|false'`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance := validateInstanceIsFound(instanceKey)
			if destinationKey == nil {
				log.Fatal("Cannot deduce target instance:", destination)
			}
			otherInstance := validateInstanceIsFound(destinationKey)

			if canReplicate, _ := instance.CanReplicateFrom(otherInstance); canReplicate {
				fmt.Println(destinationKey.DisplayString())
			}
		}
	case registerCliCommand("is-replicating", "Replication information", `Is an instance (-i) actively replicating right now`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance := validateInstanceIsFound(instanceKey)
			if instance.ReplicaRunning() {
				fmt.Println(instance.Key.DisplayString())
			}
		}
	case registerCliCommand("is-replication-stopped", "Replication information", `Is an instance (-i) a replica with both replication threads stopped`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance := validateInstanceIsFound(instanceKey)
			if instance.ReplicationThreadsStopped() {
				fmt.Println(instance.Key.DisplayString())
			}
		}
		// Instance
	case registerCliCommand("set-read-only", "Instance", `Turn an instance read-only, via SET GLOBAL read_only := 1`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := inst.SetReadOnly(instanceKey, true)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("set-writeable", "Instance", `Turn an instance writeable, via SET GLOBAL read_only := 0`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := inst.SetReadOnly(instanceKey, false)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
		// Binary log operations
	case registerCliCommand("flush-binary-logs", "Binary logs", `Flush binary logs on an instance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			var err error
			if *config.RuntimeCLIFlags.BinlogFile == "" {
				_, err = inst.FlushBinaryLogs(instanceKey, 1)
			} else {
				_, err = inst.FlushBinaryLogsTo(instanceKey, *config.RuntimeCLIFlags.BinlogFile)
			}
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("purge-binary-logs", "Binary logs", `Purge binary logs of an instance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			var err error
			if *config.RuntimeCLIFlags.BinlogFile == "" {
				log.Fatal("expecting --binlog value")
			}

			_, err = inst.PurgeBinaryLogsTo(instanceKey, *config.RuntimeCLIFlags.BinlogFile, false)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("locate-gtid-errant", "Binary logs", `List binary logs containing errant GTIDs`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			errantBinlogs, err := inst.LocateErrantGTID(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			for _, binlog := range errantBinlogs {
				fmt.Println(binlog)
			}
		}
		// Pool
	case registerCliCommand("submit-pool-instances", "Pools", `Submit a pool name with a list of instances in that pool`):
		{
			if pool == "" {
				log.Fatal("Please submit --pool")
			}
			err := inst.ApplyPoolInstances(inst.NewPoolInstancesSubmission(pool, instance))
			if err != nil {
				log.Fatal(err)
			}
		}
	case registerCliCommand("cluster-pool-instances", "Pools", `List all pools and their associated instances`):
		{
			clusterPoolInstances, err := inst.ReadAllClusterPoolInstances()
			if err != nil {
				log.Fatal(err)
			}
			for _, clusterPoolInstance := range clusterPoolInstances {
				fmt.Printf("%s\t%s\t%s\t%s:%d\n", clusterPoolInstance.ClusterName, clusterPoolInstance.ClusterAlias, clusterPoolInstance.Pool, clusterPoolInstance.Hostname, clusterPoolInstance.Port)
			}
		}
	case registerCliCommand("which-heuristic-cluster-pool-instances", "Pools", `List instances of a given cluster which are in either any pool or in a specific pool`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)

			instances, err := inst.GetHeuristicClusterPoolInstances(clusterName, pool)
			if err != nil {
				log.Fatal(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		}
		// Information
	case registerCliCommand("find", "Information", `Find instances whose hostname matches given regex pattern`):
		{
			if pattern == "" {
				log.Fatal("No pattern given")
			}
			instances, err := inst.FindInstances(pattern)
			if err != nil {
				log.Fatal(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("search", "Information", `Search instances by name, version, version comment, port`):
		{
			if pattern == "" {
				log.Fatal("No pattern given")
			}
			instances, err := inst.SearchInstances(pattern)
			if err != nil {
				log.Fatal(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("clusters", "Information", `List all clusters known to orchestrator`):
		{
			clusters, err := inst.ReadClusters()
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(strings.Join(clusters, "\n"))
		}
	case registerCliCommand("clusters-alias", "Information", `List all clusters known to orchestrator`):
		{
			clusters, err := inst.ReadClustersInfo("")
			if err != nil {
				log.Fatal(err)
			}
			for _, cluster := range clusters {
				fmt.Printf("%s\t%s\n", cluster.ClusterName, cluster.ClusterAlias)
			}
		}
	case registerCliCommand("all-clusters-primaries", "Information", `List of writeable primaries, one per cluster`):
		{
			instances, err := inst.ReadWriteableClustersPrimaries()
			if err != nil {
				log.Fatal(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("topology", "Information", `Show an ascii-graph of a replication topology, given a member of that topology`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			output, err := inst.ASCIITopology(clusterName, pattern, false, false)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(output)
		}
	case registerCliCommand("topology-tabulated", "Information", `Show an ascii-graph of a replication topology, given a member of that topology`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			output, err := inst.ASCIITopology(clusterName, pattern, true, false)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(output)
		}
	case registerCliCommand("topology-tags", "Information", `Show an ascii-graph of a replication topology and instance tags, given a member of that topology`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			output, err := inst.ASCIITopology(clusterName, pattern, false, true)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(output)
		}
	case registerCliCommand("all-instances", "Information", `The complete list of known instances`):
		{
			instances, err := inst.SearchInstances("")
			if err != nil {
				log.Fatal(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		}
	case registerCliCommand("which-instance", "Information", `Output the fully-qualified hostname:port representation of the given instance, or error if unknown`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unable to get primary: unresolved instance")
			}
			instance := validateInstanceIsFound(instanceKey)
			fmt.Println(instance.Key.DisplayString())
		}
	case registerCliCommand("which-cluster", "Information", `Output the name of the cluster an instance belongs to, or error if unknown to orchestrator`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			fmt.Println(clusterName)
		}
	case registerCliCommand("which-cluster-alias", "Information", `Output the alias of the cluster an instance belongs to, or error if unknown to orchestrator`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			clusterInfo, err := inst.ReadClusterInfo(clusterName)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(clusterInfo.ClusterAlias)
		}
	case registerCliCommand("which-cluster-domain", "Information", `Output the domain name of the cluster an instance belongs to, or error if unknown to orchestrator`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			clusterInfo, err := inst.ReadClusterInfo(clusterName)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(clusterInfo.ClusterDomain)
		}
	case registerCliCommand("which-heuristic-domain-instance", "Information", `Returns the instance associated as the cluster's writer with a cluster's domain name.`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			instanceKey, err := inst.GetHeuristicClusterDomainInstanceAttribute(clusterName)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("which-cluster-primary", "Information", `Output the name of the primary in a given cluster`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			primaries, err := inst.ReadClusterPrimary(clusterName)
			if err != nil {
				log.Fatal(err)
			}
			if len(primaries) == 0 {
				log.Fatalf("No writeable primaries found for cluster %+v", clusterName)
			}
			fmt.Println(primaries[0].Key.DisplayString())
		}
	case registerCliCommand("which-cluster-instances", "Information", `Output the list of instances participating in same cluster as given instance`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			instances, err := inst.ReadClusterInstances(clusterName)
			if err != nil {
				log.Fatal(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.Key.DisplayString())
			}
		}
	case registerCliCommand("which-cluster-osc-replicas", "Information", `Output a list of replicas in a cluster, that could serve as a pt-online-schema-change operation control replicas`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			instances, err := inst.GetClusterOSCReplicas(clusterName)
			if err != nil {
				log.Fatal(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.Key.DisplayString())
			}
		}
	case registerCliCommand("which-cluster-gh-ost-replicas", "Information", `Output a list of replicas in a cluster, that could serve as a gh-ost working server`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			instances, err := inst.GetClusterGhostReplicas(clusterName)
			if err != nil {
				log.Fatal(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.Key.DisplayString())
			}
		}
	case registerCliCommand("which-primary", "Information", `Output the fully-qualified hostname:port representation of a given instance's primary`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unable to get primary: unresolved instance")
			}
			instance := validateInstanceIsFound(instanceKey)
			if instance.SourceKey.IsValid() {
				fmt.Println(instance.SourceKey.DisplayString())
			}
		}
	case registerCliCommand("which-downtimed-instances", "Information", `List instances currently downtimed, potentially filtered by cluster`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			instances, err := inst.ReadDowntimedInstances(clusterName)
			if err != nil {
				log.Fatal(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.Key.DisplayString())
			}
		}
	case registerCliCommand("which-replicas", "Information", `Output the fully-qualified hostname:port list of replicas of a given instance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unable to get replicas: unresolved instance")
			}
			replicas, err := inst.ReadReplicaInstances(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			for _, replica := range replicas {
				fmt.Println(replica.Key.DisplayString())
			}
		}
	case registerCliCommand("which-lost-in-recovery", "Information", `List instances marked as downtimed for being lost in a recovery process`):
		{
			instances, err := inst.ReadLostInRecoveryInstances("")
			if err != nil {
				log.Fatal(err)
			}
			for _, instance := range instances {
				fmt.Println(instance.Key.DisplayString())
			}
		}
	case registerCliCommand("instance-status", "Information", `Output short status on a given instance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unable to get status: unresolved instance")
			}
			instance := validateInstanceIsFound(instanceKey)
			fmt.Println(instance.HumanReadableDescription())
		}
	case registerCliCommand("get-cluster-heuristic-lag", "Information", `For a given cluster (indicated by an instance or alias), output a heuristic "representative" lag of that cluster`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			lag, err := inst.GetClusterHeuristicLag(clusterName)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(lag)
		}
	case registerCliCommand("tags", "tags", `List tags for a given instance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			tags, err := inst.ReadInstanceTags(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			for _, tag := range tags {
				fmt.Println(tag.String())
			}
		}
	case registerCliCommand("tag-value", "tags", `Get tag value for a specific instance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			tag, err := inst.ParseTag(*config.RuntimeCLIFlags.Tag)
			if err != nil {
				log.Fatal(err)
			}

			tagExists, err := inst.ReadInstanceTag(instanceKey, tag)
			if err != nil {
				log.Fatal(err)
			}
			if tagExists {
				fmt.Println(tag.TagValue)
			}
		}
	case registerCliCommand("tagged", "tags", `List instances tagged by tag-string. Format: "tagname" or "tagname=tagvalue" or comma separated "tag0,tag1=val1,tag2" for intersection of all.`):
		{
			tagsString := *config.RuntimeCLIFlags.Tag
			instanceKeyMap, err := inst.GetInstanceKeysByTags(tagsString)
			if err != nil {
				log.Fatal(err)
			}
			keysDisplayStrings := []string{}
			for _, key := range instanceKeyMap.GetInstanceKeys() {
				keysDisplayStrings = append(keysDisplayStrings, key.DisplayString())
			}
			sort.Strings(keysDisplayStrings)
			for _, s := range keysDisplayStrings {
				fmt.Println(s)
			}
		}
	case registerCliCommand("tag", "tags", `Add a tag to a given instance. Tag in "tagname" or "tagname=tagvalue" format`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			tag, err := inst.ParseTag(*config.RuntimeCLIFlags.Tag)
			if err != nil {
				log.Fatal(err)
			}
			_ = inst.PutInstanceTag(instanceKey, tag)
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("untag", "tags", `Remove a tag from an instance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			tag, err := inst.ParseTag(*config.RuntimeCLIFlags.Tag)
			if err != nil {
				log.Fatal(err)
			}
			untagged, err := inst.Untag(instanceKey, tag)
			if err != nil {
				log.Fatal(err)
			}
			for _, key := range untagged.GetInstanceKeys() {
				fmt.Println(key.DisplayString())
			}
		}
	case registerCliCommand("untag-all", "tags", `Remove a tag from all matching instances`):
		{
			tag, err := inst.ParseTag(*config.RuntimeCLIFlags.Tag)
			if err != nil {
				log.Fatal(err)
			}
			untagged, err := inst.Untag(nil, tag)
			if err != nil {
				log.Fatal(err)
			}
			for _, key := range untagged.GetInstanceKeys() {
				fmt.Println(key.DisplayString())
			}
		}

		// Instance management
	case registerCliCommand("discover", "Instance management", `Lookup an instance, investigate it`):
		{
			if instanceKey == nil {
				instanceKey = thisInstanceKey
			}
			if instanceKey == nil {
				log.Fatalf("Cannot figure instance key")
			}
			instance, err := inst.ReadTopologyInstance(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instance.Key.DisplayString())
		}
	case registerCliCommand("forget", "Instance management", `Forget about an instance's existence`):
		{
			if rawInstanceKey == nil { //nolint
				log.Fatal("Cannot deduce instance:", instance)
			}
			instanceKey, _ = inst.FigureInstanceKey(rawInstanceKey, nil)
			err := inst.ForgetInstance(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("begin-maintenance", "Instance management", `Request a maintenance lock on an instance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if reason == "" {
				log.Fatal("--reason option required")
			}
			var durationSeconds int
			if duration != "" {
				durationSeconds, err = util.SimpleTimeToSeconds(duration)
				if err != nil {
					log.Fatal(err)
				}
				if durationSeconds < 0 {
					log.Fatalf("Duration value must be non-negative. Given value: %d", durationSeconds)
				}
			}
			maintenanceKey, err := inst.BeginBoundedMaintenance(instanceKey, inst.GetMaintenanceOwner(), reason, uint(durationSeconds), true)
			if err == nil {
				log.Infof("Maintenance key: %+v", maintenanceKey)
				log.Infof("Maintenance duration: %d seconds", durationSeconds)
			}
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("end-maintenance", "Instance management", `Remove maintenance lock from an instance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := inst.EndMaintenanceByInstanceKey(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("in-maintenance", "Instance management", `Check whether instance is under maintenance`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			inMaintenance, err := inst.InMaintenance(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			if inMaintenance {
				fmt.Println(instanceKey.DisplayString())
			}
		}
	case registerCliCommand("begin-downtime", "Instance management", `Mark an instance as downtimed`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if reason == "" {
				log.Fatal("--reason option required")
			}
			var durationSeconds int
			if duration != "" {
				durationSeconds, err = util.SimpleTimeToSeconds(duration)
				if err != nil {
					log.Fatal(err)
				}
				if durationSeconds < 0 {
					log.Fatalf("Duration value must be non-negative. Given value: %d", durationSeconds)
				}
			}
			duration := time.Duration(durationSeconds) * time.Second
			err := inst.BeginDowntime(inst.NewDowntime(instanceKey, inst.GetMaintenanceOwner(), reason, duration))
			if err == nil {
				log.Infof("Downtime duration: %d seconds", durationSeconds)
			} else {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("end-downtime", "Instance management", `Indicate an instance is no longer downtimed`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			_, err := inst.EndDowntime(instanceKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
		// Recovery & analysis
	case registerCliCommand("recover", "Recovery", `Do auto-recovery given a dead instance`), registerCliCommand("recover-lite", "Recovery", `Do auto-recovery given a dead instance. Orchestrator chooses the best course of actionwithout executing external processes`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", instance)
			}

			recoveryAttempted, promotedInstanceKey, err := logic.CheckAndRecover(instanceKey, destinationKey, (command == "recover-lite"))
			if err != nil {
				log.Fatal(err)
			}
			if recoveryAttempted {
				if promotedInstanceKey == nil {
					log.Fatalf("Recovery attempted yet no replica promoted")
				}
				fmt.Println(promotedInstanceKey.DisplayString())
			}
		}
	case registerCliCommand("force-primary-failover", "Recovery", `Forcibly discard primary and initiate a failover, even if orchestrator doesn't see a problem. This command lets orchestrator choose the replacement primary`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			topologyRecovery, err := logic.ForcePrimaryFailover(clusterName)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(topologyRecovery.SuccessorKey.DisplayString())
		}
	case registerCliCommand("force-primary-takeover", "Recovery", `Forcibly discard primary and promote another (direct child) instance instead, even if everything is running well`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			if destinationKey == nil {
				log.Fatal("Cannot deduce destination, the instance to promote in place of the primary. Please provide with -d")
			}
			destination := validateInstanceIsFound(destinationKey)
			topologyRecovery, err := logic.ForcePrimaryTakeover(clusterName, destination)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(topologyRecovery.SuccessorKey.DisplayString())
		}
	case registerCliCommand("graceful-primary-takeover", "Recovery", `Gracefully promote a new primary. Either indicate identity of new primary via '-d designated.instance.com' or setup replication tree to have a single direct replica to the primary.`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			if destinationKey != nil {
				validateInstanceIsFound(destinationKey)
			}
			topologyRecovery, err := logic.GracefulPrimaryTakeover(clusterName, destinationKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(topologyRecovery.SuccessorKey.DisplayString())
			log.Infof("Promoted %+v as new primary.", topologyRecovery.SuccessorKey)
		}
	case registerCliCommand("graceful-primary-takeover-auto", "Recovery", `Gracefully promote a new primary. orchestrator will attempt to pick the promoted replica automatically`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			// destinationKey doesn't _have_ to be specified: if unspecified, orchestrator will auto-deduce a replica.
			// but if specified, then that's the replica to promote, and it must be valid.
			if destinationKey != nil {
				validateInstanceIsFound(destinationKey)
			}
			topologyRecovery, err := logic.GracefulPrimaryTakeover(clusterName, destinationKey)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(topologyRecovery.SuccessorKey.DisplayString())
			log.Infof("Promoted %+v as new primary.", topologyRecovery.SuccessorKey)
		}
	case registerCliCommand("replication-analysis", "Recovery", `Request an analysis of potential crash incidents in all known topologies`):
		{
			analysis, err := inst.GetReplicationAnalysis("", &inst.ReplicationAnalysisHints{})
			if err != nil {
				log.Fatal(err)
			}
			for _, entry := range analysis {
				fmt.Printf("%s (cluster %s): %s\n", entry.AnalyzedInstanceKey.DisplayString(), entry.ClusterDetails.ClusterName, entry.AnalysisString())
			}
		}
	case registerCliCommand("ack-all-recoveries", "Recovery", `Acknowledge all recoveries; this unblocks pending future recoveries`):
		{
			if reason == "" {
				log.Fatal("--reason option required (comment your ack)")
			}
			countRecoveries, err := logic.AcknowledgeAllRecoveries(inst.GetMaintenanceOwner(), reason)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%d recoveries acknowldged\n", countRecoveries)
		}
	case registerCliCommand("ack-cluster-recoveries", "Recovery", `Acknowledge recoveries for a given cluster; this unblocks pending future recoveries`):
		{
			if reason == "" {
				log.Fatal("--reason option required (comment your ack)")
			}
			clusterName := getClusterName(clusterAlias, instanceKey)
			countRecoveries, err := logic.AcknowledgeClusterRecoveries(clusterName, inst.GetMaintenanceOwner(), reason)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%d recoveries acknowldged\n", countRecoveries)
		}
	case registerCliCommand("ack-instance-recoveries", "Recovery", `Acknowledge recoveries for a given instance; this unblocks pending future recoveries`):
		{
			if reason == "" {
				log.Fatal("--reason option required (comment your ack)")
			}
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)

			countRecoveries, err := logic.AcknowledgeInstanceRecoveries(instanceKey, inst.GetMaintenanceOwner(), reason)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%d recoveries acknowldged\n", countRecoveries)
		}
	// Instance meta
	case registerCliCommand("register-candidate", "Instance, meta", `Indicate that a specific instance is a preferred candidate for primary promotion`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			promotionRule, err := promotionrule.Parse(*config.RuntimeCLIFlags.PromotionRule)
			if err != nil {
				log.Fatal(err)
			}
			err = inst.RegisterCandidateInstance(inst.NewCandidateDatabaseInstance(instanceKey, promotionRule).WithCurrentTime())
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("register-hostname-unresolve", "Instance, meta", `Assigns the given instance a virtual (aka "unresolved") name`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			err := inst.RegisterHostnameUnresolve(inst.NewHostnameRegistration(instanceKey, hostnameFlag))
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("deregister-hostname-unresolve", "Instance, meta", `Explicitly deregister/dosassociate a hostname with an "unresolved" name`):
		{
			instanceKey, _ = inst.FigureInstanceKey(instanceKey, thisInstanceKey)
			err := inst.RegisterHostnameUnresolve(inst.NewHostnameDeregistration(instanceKey))
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	case registerCliCommand("set-heuristic-domain-instance", "Instance, meta", `Associate domain name of given cluster with what seems to be the writer primary for that cluster`):
		{
			clusterName := getClusterName(clusterAlias, instanceKey)
			instanceKey, err := inst.HeuristicallyApplyClusterDomainInstanceAttribute(clusterName)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}

		// meta
	case registerCliCommand("snapshot-topologies", "Meta", `Take a snapshot of existing topologies.`):
		{
			err := inst.SnapshotTopologies()
			if err != nil {
				log.Fatal(err)
			}
		}
	case registerCliCommand("continuous", "Meta", `Enter continuous mode, and actively poll for instances, diagnose problems, do maintenance`):
		{
			logic.ContinuousDiscovery()
		}
	case registerCliCommand("active-nodes", "Meta", `List currently active orchestrator nodes`):
		{
			nodes, err := process.ReadAvailableNodes(false)
			if err != nil {
				log.Fatal(err)
			}
			for _, node := range nodes {
				fmt.Println(node)
			}
		}
	case registerCliCommand("access-token", "Meta", `Get a HTTP access token`):
		{
			publicToken, err := process.GenerateAccessToken(owner)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(publicToken)
		}
	case registerCliCommand("resolve", "Meta", `Resolve given hostname`):
		{
			if rawInstanceKey == nil { //nolint
				log.Fatal("Cannot deduce instance:", instance)
			}
			if conn, err := net.Dial("tcp", rawInstanceKey.DisplayString()); err == nil {
				log.Infof("tcp test is good; got connection %+v", conn)
				_ = conn.Close()
			} else {
				log.Fatal(err)
			}
			if cname, err := inst.GetCNAME(rawInstanceKey.Hostname); err == nil { //nolint
				log.Infof("GetCNAME() %+v, %+v", cname, err)
				rawInstanceKey.Hostname = cname
				fmt.Println(rawInstanceKey.DisplayString())
			} else {
				log.Fatal(err)
			}
		}
	case registerCliCommand("reset-hostname-resolve-cache", "Meta", `Clear the hostname resolve cache`):
		{
			err := inst.ResetHostnameResolveCache()
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("hostname resolve cache cleared")
		}
	case registerCliCommand("dump-config", "Meta", `Print out configuration in JSON format`):
		{
			jsonString := config.Config.ToJSONString()
			fmt.Println(jsonString)
		}
	case registerCliCommand("show-resolve-hosts", "Meta", `Show the content of the hostname_resolve table. Generally used for debugging`):
		{
			resolves, err := inst.ReadAllHostnameResolves()
			if err != nil {
				log.Fatal(err)
			}
			for _, r := range resolves {
				fmt.Println(r)
			}
		}
	case registerCliCommand("show-unresolve-hosts", "Meta", `Show the content of the hostname_unresolve table. Generally used for debugging`):
		{
			unresolves, err := inst.ReadAllHostnameUnresolves()
			if err != nil {
				log.Fatal(err)
			}
			for _, r := range unresolves {
				fmt.Println(r)
			}
		}
	case registerCliCommand("redeploy-internal-db", "Meta, internal", `Force internal schema migration to current backend structure`):
		{
			config.RuntimeCLIFlags.ConfiguredVersion = ""
			_, err := inst.ReadClusters()
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("Redeployed internal db")
		}
	case registerCliCommand("internal-suggest-promoted-replacement", "Internal", `Internal only, used to test promotion logic in CI`):
		{
			destination := validateInstanceIsFound(destinationKey)
			replacement, _, err := logic.SuggestReplacementForPromotedReplica(&logic.TopologyRecovery{}, instanceKey, destination, nil)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(replacement.Key.DisplayString())
		}
	case registerCliCommand("disable-global-recoveries", "", `Disallow orchestrator from performing recoveries globally`):
		{
			if err := logic.DisableRecovery(); err != nil {
				log.Fatalf("ERROR: Failed to disable recoveries globally: %v\n", err)
			}
			fmt.Println("OK: Orchestrator recoveries DISABLED globally")
		}
	case registerCliCommand("enable-global-recoveries", "", `Allow orchestrator to perform recoveries globally`):
		{
			if err := logic.EnableRecovery(); err != nil {
				log.Fatalf("ERROR: Failed to enable recoveries globally: %v\n", err)
			}
			fmt.Println("OK: Orchestrator recoveries ENABLED globally")
		}
	case registerCliCommand("check-global-recoveries", "", `Show the global recovery configuration`):
		{
			isDisabled, err := logic.IsRecoveryDisabled()
			if err != nil {
				log.Fatalf("ERROR: Failed to determine if recoveries are disabled globally: %v\n", err)
			}
			fmt.Printf("OK: Global recoveries disabled: %v\n", isDisabled)
		}
	case registerCliCommand("bulk-instances", "", `Return a list of sorted instance names known to orchestrator`):
		{
			instances, err := inst.BulkReadInstance()
			if err != nil {
				log.Fatalf("Error: Failed to retrieve instances: %v\n", err)
				return
			}
			var asciiInstances stringSlice
			for _, v := range instances {
				asciiInstances = append(asciiInstances, v.String())
			}
			sort.Sort(asciiInstances)
			fmt.Printf("%s\n", strings.Join(asciiInstances, "\n"))
		}
	case registerCliCommand("bulk-promotion-rules", "", `Return a list of promotion rules known to orchestrator`):
		{
			promotionRules, err := inst.BulkReadCandidateDatabaseInstance()
			if err != nil {
				log.Fatalf("Error: Failed to retrieve promotion rules: %v\n", err)
			}
			var asciiPromotionRules stringSlice
			for _, v := range promotionRules {
				asciiPromotionRules = append(asciiPromotionRules, v.String())
			}
			sort.Sort(asciiPromotionRules)

			fmt.Printf("%s\n", strings.Join(asciiPromotionRules, "\n"))
		}
		// Help
	case "help":
		{
			_, _ = fmt.Fprint(os.Stderr, availableCommandsUsage())
		}
	default:
		log.Fatalf("Unknown command: \"%s\". %s", command, availableCommandsUsage())
	}
}
