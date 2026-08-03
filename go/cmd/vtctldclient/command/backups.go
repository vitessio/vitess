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

package command

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/topoproto"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// emptyBackupExitCode is returned by the Backup/BackupShard commands when an
// incremental backup completes with no new data to back up. It lets callers skip
// follow-up operations by checking $? without parsing output.
const emptyBackupExitCode = 2

var (
	// Backup makes a Backup gRPC call to a vtctld.
	Backup = &cobra.Command{
		Use:                   "Backup [--concurrency <concurrency>] [--allow-primary] [--incremental-from-pos=<pos>|<backup-name>|auto] [--upgrade-safe] [--backup-engine=enginename] <tablet_alias>",
		Short:                 "Uses the BackupStorage service on the given tablet to create and store a new backup.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandBackup,
	}
	// BackupShard makes a BackupShard gRPC call to a vtctld.
	BackupShard = &cobra.Command{
		Use:   "BackupShard [--concurrency <concurrency>] [--allow-primary] [--incremental-from-pos=<pos>|<backup-name>|auto] [--upgrade-safe] <keyspace/shard>",
		Short: "Finds the most up-to-date REPLICA, RDONLY, or SPARE tablet in the given shard and uses the BackupStorage service on that tablet to create and store a new backup.",
		Long: `Finds the most up-to-date REPLICA, RDONLY, or SPARE tablet in the given shard and uses the BackupStorage service on that tablet to create and store a new backup.

If no replica-type tablet can be found, the backup can be taken on the primary if --allow-primary is specified.`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandBackupShard,
	}
	// GetBackups makes a GetBackups gRPC call to a vtctld.
	GetBackups = &cobra.Command{
		Use:                   "GetBackups [--limit <limit>] [--json] <keyspace/shard>",
		Short:                 "Lists backups for the given shard.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetBackups,
	}
	// RemoveBackup makes a RemoveBackup gRPC call to a vtctld.
	RemoveBackup = &cobra.Command{
		Use:                   "RemoveBackup <keyspace/shard> <backup name>",
		Short:                 "Removes the given backup from the BackupStorage used by vtctld.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandRemoveBackup,
	}
	// RestoreFromBackup makes a RestoreFromBackup gRPC call to a vtctld.
	RestoreFromBackup = &cobra.Command{
		Use:                   "RestoreFromBackup [--backup-timestamp|-t <YYYY-mm-DD.HHMMSS>] [--restore-to-pos <pos>] [--allowed-backup-engines=enginename,] [--dry-run] <tablet_alias>",
		Short:                 "Stops mysqld on the specified tablet and restores the data from either the latest backup or closest before `backup-timestamp`.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandRestoreFromBackup,
	}
)

var backupOptions = struct {
	AllowPrimary         bool
	BackupEngine         string
	Concurrency          int32
	IncrementalFromPos   string
	UpgradeSafe          bool
	MysqlShutdownTimeout time.Duration
	InitSQLQueries       []string
	InitSQLTabletTypes   []topodatapb.TabletType
	InitSQLTimeout       time.Duration
	InitSQLFailOnError   bool
	OutputJSON           bool
}{}

func validateBackupOptions() error {
	if len(backupOptions.InitSQLQueries) > 0 {
		if len(backupOptions.InitSQLTabletTypes) == 0 {
			return errors.New("backup init SQL queries provided but no tablet types on which to run them")
		}
		if backupOptions.InitSQLTimeout == 0 {
			return errors.New("backup init SQL queries provided but no timeout provided -- this is dangerous and not allowed")
		}
	}
	return nil
}

func commandBackup(cmd *cobra.Command, args []string) error {
	tabletAlias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	if err := validateBackupOptions(); err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	req := &vtctldatapb.BackupRequest{
		TabletAlias:          tabletAlias,
		AllowPrimary:         backupOptions.AllowPrimary,
		Concurrency:          backupOptions.Concurrency,
		IncrementalFromPos:   backupOptions.IncrementalFromPos,
		UpgradeSafe:          backupOptions.UpgradeSafe,
		MysqlShutdownTimeout: protoutil.DurationToProto(backupOptions.MysqlShutdownTimeout),
		InitSql: &tabletmanagerdatapb.BackupRequest_InitSQL{
			Queries:     backupOptions.InitSQLQueries,
			TabletTypes: backupOptions.InitSQLTabletTypes,
			Timeout:     protoutil.DurationToProto(backupOptions.InitSQLTimeout),
			FailOnError: backupOptions.InitSQLFailOnError,
		},
	}

	if backupOptions.BackupEngine != "" {
		req.BackupEngine = &backupOptions.BackupEngine
	}

	stream, err := client.Backup(commandCtx, req)
	if err != nil {
		return err
	}

	return handleBackupStream(stream, backupOptions.OutputJSON)
}

var backupShardOptions = struct {
	AllowPrimary         bool
	Concurrency          int32
	IncrementalFromPos   string
	UpgradeSafe          bool
	MysqlShutdownTimeout time.Duration
	OutputJSON           bool
}{}

func commandBackupShard(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	if err := validateBackupOptions(); err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	stream, err := client.BackupShard(commandCtx, &vtctldatapb.BackupShardRequest{
		Keyspace:             keyspace,
		Shard:                shard,
		AllowPrimary:         backupShardOptions.AllowPrimary,
		Concurrency:          backupShardOptions.Concurrency,
		IncrementalFromPos:   backupShardOptions.IncrementalFromPos,
		UpgradeSafe:          backupShardOptions.UpgradeSafe,
		MysqlShutdownTimeout: protoutil.DurationToProto(backupShardOptions.MysqlShutdownTimeout),
		InitSql: &tabletmanagerdatapb.BackupRequest_InitSQL{
			Queries:     backupOptions.InitSQLQueries,
			TabletTypes: backupOptions.InitSQLTabletTypes,
			Timeout:     protoutil.DurationToProto(backupOptions.InitSQLTimeout),
			FailOnError: backupOptions.InitSQLFailOnError,
		},
	})
	if err != nil {
		return err
	}

	return handleBackupStream(stream, backupShardOptions.OutputJSON)
}

// backupResponseStream is the common Recv interface of the Backup and
// BackupShard client streams.
type backupResponseStream interface {
	Recv() (*vtctldatapb.BackupResponse, error)
}

// backupJSONOutput is the structure printed to stdout by Backup/BackupShard in
// --json mode.
type backupJSONOutput struct {
	// Status is the terminal outcome: "USABLE", "EMPTY", or "UNKNOWN".
	Status string `json:"status"`
	// Manifest is the backup's MANIFEST as raw JSON, or null for an empty backup
	// or when talking to an older server that does not return it.
	Manifest json.RawMessage `json:"manifest"`
}

// handleBackupStream drains a Backup/BackupShard stream, printing progress and,
// on completion, the backup's MANIFEST and outcome. When the backup is an empty
// (no-op) incremental backup it exits the process with emptyBackupExitCode so
// callers can skip follow-up work by checking $?.
func handleBackupStream(stream backupResponseStream, outputJSON bool) error {
	status, err := consumeBackupStream(stream, outputJSON, os.Stdout, os.Stderr)
	if err != nil {
		return err
	}
	if status == tabletmanagerdatapb.BackupResponse_EMPTY {
		exit.Return(emptyBackupExitCode)
	}
	return nil
}

// consumeBackupStream reads all messages from a backup stream. Log events are
// written to errOut in JSON mode (keeping out clean for the final JSON object)
// and to out otherwise. On completion it prints the manifest/status and returns
// the terminal status. It never calls exit, so it is safe to unit test.
func consumeBackupStream(stream backupResponseStream, outputJSON bool, out, errOut io.Writer) (tabletmanagerdatapb.BackupResponse_Status, error) {
	var (
		manifest string
		status   = tabletmanagerdatapb.BackupResponse_STATUS_UNSPECIFIED
	)

	for {
		resp, err := stream.Recv()
		switch err {
		case nil:
			if resp.Event != nil {
				line := fmt.Sprintf("%s/%s (%s): %v\n", resp.Keyspace, resp.Shard, topoproto.TabletAliasString(resp.TabletAlias), resp.Event)
				if outputJSON {
					fmt.Fprint(errOut, line)
				} else {
					fmt.Fprint(out, line)
				}
			}
			if resp.Manifest != "" {
				manifest = resp.Manifest
			}
			if resp.Status != tabletmanagerdatapb.BackupResponse_STATUS_UNSPECIFIED {
				status = resp.Status
			}
		case io.EOF:
			if perr := printBackupResult(out, outputJSON, manifest, status); perr != nil {
				return status, perr
			}
			return status, nil
		default:
			return status, err
		}
	}
}

// printBackupResult writes the backup's MANIFEST and outcome to out.
func printBackupResult(out io.Writer, outputJSON bool, manifest string, status tabletmanagerdatapb.BackupResponse_Status) error {
	if outputJSON {
		raw := json.RawMessage("null")
		if manifest != "" {
			raw = json.RawMessage(manifest)
		}
		data, err := json.MarshalIndent(backupJSONOutput{
			Status:   backupStatusString(status),
			Manifest: raw,
		}, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "%s\n", data)
		return nil
	}

	switch status {
	case tabletmanagerdatapb.BackupResponse_EMPTY:
		fmt.Fprintln(out, mysqlctl.EmptyBackupMessage)
	default:
		if manifest != "" {
			fmt.Fprintf(out, "%s\n", manifest)
		}
	}
	return nil
}

// backupStatusString renders a backup status for human/JSON output.
func backupStatusString(status tabletmanagerdatapb.BackupResponse_Status) string {
	switch status {
	case tabletmanagerdatapb.BackupResponse_USABLE:
		return "USABLE"
	case tabletmanagerdatapb.BackupResponse_EMPTY:
		return "EMPTY"
	default:
		return "UNKNOWN"
	}
}

var getBackupsOptions = struct {
	Limit      uint32
	OutputJSON bool
}{}

func commandGetBackups(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.GetBackups(commandCtx, &vtctldatapb.GetBackupsRequest{
		Keyspace: keyspace,
		Shard:    shard,
		Limit:    getBackupsOptions.Limit,
	})
	if err != nil {
		return err
	}

	if getBackupsOptions.OutputJSON {
		data, err := cli.MarshalJSON(resp)
		if err != nil {
			return err
		}

		fmt.Printf("%s\n", data)
		return nil
	}

	names := make([]string, len(resp.Backups))
	for i, b := range resp.Backups {
		names[i] = b.Name
	}

	fmt.Printf("%s\n", strings.Join(names, "\n"))

	return nil
}

func commandRemoveBackup(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	name := cmd.Flags().Arg(1)

	cli.FinishedParsing(cmd)

	_, err = client.RemoveBackup(commandCtx, &vtctldatapb.RemoveBackupRequest{
		Keyspace: keyspace,
		Shard:    shard,
		Name:     name,
	})
	return err
}

var restoreFromBackupOptions = struct {
	BackupTimestamp      string
	AllowedBackupEngines []string
	RestoreToPos         string
	RestoreToTimestamp   string
	DryRun               bool
}{}

func commandRestoreFromBackup(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	if restoreFromBackupOptions.RestoreToPos != "" && restoreFromBackupOptions.RestoreToTimestamp != "" {
		return errors.New("--restore-to-pos and --restore-to-timestamp are mutually exclusive")
	}

	var restoreToTimestamp time.Time
	if restoreFromBackupOptions.RestoreToTimestamp != "" {
		restoreToTimestamp, err = mysqlctl.ParseRFC3339(restoreFromBackupOptions.RestoreToTimestamp)
		if err != nil {
			return err
		}
	}

	req := &vtctldatapb.RestoreFromBackupRequest{
		TabletAlias:          alias,
		RestoreToPos:         restoreFromBackupOptions.RestoreToPos,
		RestoreToTimestamp:   protoutil.TimeToProto(restoreToTimestamp),
		DryRun:               restoreFromBackupOptions.DryRun,
		AllowedBackupEngines: restoreFromBackupOptions.AllowedBackupEngines,
	}

	if restoreFromBackupOptions.BackupTimestamp != "" {
		t, err := time.Parse(mysqlctl.BackupTimestampFormat, restoreFromBackupOptions.BackupTimestamp)
		if err != nil {
			return err
		}

		req.BackupTime = protoutil.TimeToProto(t)
	}

	cli.FinishedParsing(cmd)

	stream, err := client.RestoreFromBackup(commandCtx, req)
	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		switch err {
		case nil:
			fmt.Printf("%s/%s (%s): %v\n", resp.Keyspace, resp.Shard, topoproto.TabletAliasString(resp.TabletAlias), resp.Event)
		case io.EOF:
			return nil
		default:
			return err
		}
	}
}

func init() {
	Backup.Flags().BoolVar(&backupOptions.AllowPrimary, "allow-primary", false, "Allow the primary of a shard to be used for the backup. WARNING: If using the builtin backup engine, this will shutdown mysqld on the primary and stop writes for the duration of the backup.")
	Backup.Flags().Int32Var(&backupOptions.Concurrency, "concurrency", 4, "Specifies the number of compression/checksum jobs to run simultaneously.")
	Backup.Flags().StringVar(&backupOptions.IncrementalFromPos, "incremental-from-pos", "", "Position, or name of backup from which to create an incremental backup. Default: empty. If given, then this backup becomes an incremental backup from given position or given backup. If value is 'auto', this backup will be taken from the last successful backup position.")
	Backup.Flags().StringVar(&backupOptions.BackupEngine, "backup-engine", "", "Request a specific backup engine for this backup request. Defaults to the preferred backup engine of the target vttablet")

	Backup.Flags().BoolVar(&backupOptions.UpgradeSafe, "upgrade-safe", false, "Whether to use innodb_fast_shutdown=0 for the backup so it is safe to use for MySQL upgrades.")
	Backup.Flags().DurationVar(&backupOptions.MysqlShutdownTimeout, "mysql-shutdown-timeout", mysqlctl.DefaultShutdownTimeout, "Timeout to use when MySQL is being shut down.")
	Backup.Flags().BoolVarP(&backupOptions.OutputJSON, "json", "j", false, "Output, on completion, the backup's MANIFEST and outcome status as JSON on stdout (log events go to stderr). An empty incremental backup exits with a distinct code.")
	addInitSQLFlags(Backup)
	Root.AddCommand(Backup)

	BackupShard.Flags().BoolVar(&backupShardOptions.AllowPrimary, "allow-primary", false, "Allow the primary of a shard to be used for the backup. WARNING: If using the builtin backup engine, this will shutdown mysqld on the primary and stop writes for the duration of the backup.")
	BackupShard.Flags().Int32Var(&backupShardOptions.Concurrency, "concurrency", 4, "Specifies the number of compression/checksum jobs to run simultaneously.")
	BackupShard.Flags().StringVar(&backupShardOptions.IncrementalFromPos, "incremental-from-pos", "", "Position, or name of backup from which to create an incremental backup. Default: empty. If given, then this backup becomes an incremental backup from given position or given backup. If value is 'auto', this backup will be taken from the last successful backup position.")
	BackupShard.Flags().BoolVar(&backupShardOptions.UpgradeSafe, "upgrade-safe", false, "Whether to use innodb_fast_shutdown=0 for the backup so it is safe to use for MySQL upgrades.")
	BackupShard.Flags().DurationVar(&backupShardOptions.MysqlShutdownTimeout, "mysql-shutdown-timeout", mysqlctl.DefaultShutdownTimeout, "Timeout to use when MySQL is being shut down.")
	BackupShard.Flags().BoolVarP(&backupShardOptions.OutputJSON, "json", "j", false, "Output, on completion, the backup's MANIFEST and outcome status as JSON on stdout (log events go to stderr). An empty incremental backup exits with a distinct code.")
	addInitSQLFlags(BackupShard)
	Root.AddCommand(BackupShard)

	GetBackups.Flags().Uint32VarP(&getBackupsOptions.Limit, "limit", "l", 0, "Retrieve only the most recent N backups.")
	GetBackups.Flags().BoolVarP(&getBackupsOptions.OutputJSON, "json", "j", false, "Output backup info in JSON format rather than a list of backups.")
	Root.AddCommand(GetBackups)

	Root.AddCommand(RemoveBackup)

	RestoreFromBackup.Flags().StringVarP(&restoreFromBackupOptions.BackupTimestamp, "backup-timestamp", "t", "", "Use the backup taken at, or closest before, this timestamp. Omit to use the latest backup. Timestamp format is \"YYYY-mm-DD.HHMMSS\".")
	RestoreFromBackup.Flags().StringSliceVar(&restoreFromBackupOptions.AllowedBackupEngines, "allowed-backup-engines", restoreFromBackupOptions.AllowedBackupEngines, "if set, only backups taken with the specified engines are eligible to be restored")
	RestoreFromBackup.Flags().StringVar(&restoreFromBackupOptions.RestoreToPos, "restore-to-pos", "", "Run a point in time recovery that ends with the given position. This will attempt to use one full backup followed by zero or more incremental backups")
	RestoreFromBackup.Flags().StringVar(&restoreFromBackupOptions.RestoreToTimestamp, "restore-to-timestamp", "", "Run a point in time recovery that restores up to, and excluding, given timestamp in RFC3339 format (`2006-01-02T15:04:05Z07:00`). This will attempt to use one full backup followed by zero or more incremental backups")
	RestoreFromBackup.Flags().BoolVar(&restoreFromBackupOptions.DryRun, "dry-run", false, "Only validate restore steps, do not actually restore data")
	Root.AddCommand(RestoreFromBackup)
}

func addInitSQLFlags(cmd *cobra.Command) {
	cmd.Flags().StringSliceVar(&backupOptions.InitSQLQueries, "init-backup-sql-queries", nil, "Queries to execute before taking the backup")
	cmd.Flags().Var((*topoproto.TabletTypeListFlag)(&backupOptions.InitSQLTabletTypes), "init-backup-tablet-types", "Tablet types used for the backup where the init SQL queries (--init-backup-sql-queries) will be executed before taking the backup")
	cmd.Flags().DurationVar(&backupOptions.InitSQLTimeout, "init-backup-sql-timeout", backupOptions.InitSQLTimeout, "At what point should we time out the init SQL query (--init-backup-sql-queries) work and either fail the backup job (--init-backup-sql-fail-on-error) or continue on with the backup")
	cmd.Flags().BoolVar(&backupOptions.InitSQLFailOnError, "init-backup-sql-fail-on-error", false, "Whether or not to fail the backup if the init SQL queries (--init-backup-sql-queries) fail, which includes if they fail to complete before the specified timeout (--init-backup-sql-timeout)")
}
