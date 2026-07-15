/*
Copyright 2026 The Vitess Authors.

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

package vitesst

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"vitess.io/vitess/go/mysql"
)

// vtbackupTimeout bounds how long one vtbackup run may take.
const vtbackupTimeout = 10 * time.Minute

// backupRoot is where the shared backup volume is mounted in every container
// that reads or writes backups. It lives outside VTDATAROOT because that is a
// tmpfs, and a volume mounted under it would have no image directory to take
// its ownership from.
const backupRoot = "/vt/backups"

type (
	// Vtbackup is the runtime handle for one vtbackup container. It runs to
	// completion on its own; Wait blocks until it exits.
	Vtbackup struct {
		component
	}

	// VtbackupSpec describes one vtbackup run.
	VtbackupSpec struct {
		// Keyspace and Shard the backup is taken for.
		Keyspace string
		Shard    string

		// InitialBackup takes the shard's first backup from an empty mysqld
		// instead of restoring and replicating first.
		InitialBackup bool

		// ExtraArgs are appended to the vtbackup command line.
		ExtraArgs []string
	}

	backupStorageOption struct{}

	mysqlctldOption struct{}
)

func (backupStorageOption) apply(opts *clusterOptions) {
	opts.backupStorage = true
}

func (mysqlctldOption) apply(opts *clusterOptions) {
	opts.mysqlctld = true
}

// WithMysqlctld runs every tablet's mysqld under the mysqlctld daemon instead
// of one-shot mysqlctl invocations.
func WithMysqlctld() ClusterOption {
	return mysqlctldOption{}
}

// WithBackupStorage gives the cluster a shared file backup storage: a Docker
// volume mounted at /vt/vtdataroot/backups in every tablet, vtctld, and
// vtbackup container, with the file backup-storage flags set to match.
func WithBackupStorage() ClusterOption {
	return backupStorageOption{}
}

// backupFlags are the backup-storage flags every component that touches
// backups is started with.
func (c *Cluster) backupFlags() []string {
	if !c.opts.backupStorage {
		return nil
	}
	return []string{
		"--backup-storage-implementation", "file",
		"--file-backup-storage-root", backupRoot,
	}
}

// backupMount mounts the cluster's backup volume into a container.
func (c *Cluster) backupMount() testcontainers.CustomizeRequestOption {
	return testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
		hc.Mounts = append(hc.Mounts, mount.Mount{
			Type:   mount.TypeVolume,
			Source: c.backupVolume,
			Target: backupRoot,
		})
	})
}

// createBackupVolume creates the cluster's shared backup volume.
func (c *Cluster) createBackupVolume(ctx context.Context) error {
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return fmt.Errorf("creating docker client: %w", err)
	}
	defer cli.Close()

	vol, err := cli.VolumeCreate(ctx, client.VolumeCreateOptions{
		Labels: testcontainers.GenericLabels(),
	})
	if err != nil {
		return fmt.Errorf("creating backup volume: %w", err)
	}

	c.backupVolume = vol.Volume.Name
	return nil
}

// removeBackupVolume removes the cluster's shared backup volume.
func (c *Cluster) removeBackupVolume(ctx context.Context) error {
	if c.backupVolume == "" {
		return nil
	}

	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return fmt.Errorf("creating docker client: %w", err)
	}
	defer cli.Close()

	if _, err := cli.VolumeRemove(ctx, c.backupVolume, client.VolumeRemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("removing backup volume %s: %w", c.backupVolume, err)
	}
	c.backupVolume = ""
	return nil
}

// RunVtbackup runs one vtbackup container to completion and returns its
// output. The container shares the cluster's backup volume and network.
func (c *Cluster) RunVtbackup(ctx context.Context, spec VtbackupSpec) (string, error) {
	vb, err := c.StartVtbackup(ctx, spec)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := vb.terminate(ctx); err != nil {
			c.logf("terminating %s: %v", vb.name, err)
		}
	}()

	return vb.Wait(ctx)
}

// StartVtbackup starts a vtbackup container and returns immediately, so tests
// can observe its ephemeral mysqld and files while it runs. Wait blocks until
// it exits; the container is torn down with the cluster.
func (c *Cluster) StartVtbackup(ctx context.Context, spec VtbackupSpec) (*Vtbackup, error) {
	if !c.opts.backupStorage {
		return nil, errors.New("cluster has no backup storage, use WithBackupStorage")
	}

	name := c.name(fmt.Sprintf("vtbackup-%d", c.nextTabletUID()))
	initDBPath := containerFilesDir + "/init_db.sql"

	args := []string{"vtbackup"}
	args = append(args, c.TopoFlags()...)
	args = append(args, c.backupFlags()...)
	args = append(
		args,
		"--init-keyspace", spec.Keyspace,
		"--init-shard", spec.Shard,
		"--init-db-sql-file", initDBPath,
		"--mysql-port", strconv.Itoa(tabletMySQLPort),
		"--log-format", "text",
		"--alsologtostderr",
	)
	if spec.InitialBackup {
		args = append(args, "--initial-backup")
	}
	args = append(args, spec.ExtraArgs...)

	files := []ContainerFile{{Content: []byte(c.initDBSQL), ContainerPath: initDBPath}}
	files = append(files, c.opts.tabletFiles...)
	filesOpt, err := withContainerFiles(files)
	if err != nil {
		return nil, fmt.Errorf("preparing files for %s: %w", name, err)
	}

	ctr, err := testcontainers.Run(
		ctx, c.image,
		testcontainers.WithCmd(args...),
		testcontainers.WithExposedPorts(fmt.Sprintf("%d/tcp", tabletMySQLPort)),
		network.WithNetwork([]string{name}, c.network),
		testcontainers.WithTmpfs(map[string]string{vtDataRoot: "uid=999,gid=999"}),
		testcontainers.WithEnv(mergeEnv(map[string]string{"VTTEST": "endtoend"}, c.opts.tabletEnv)),
		c.backupMount(),
		filesOpt,
		testcontainers.WithLogConsumers(c.newLogConsumer(name)),
		// vtbackup runs to completion on its own; nothing to wait for at start.
		testcontainers.WithWaitStrategy(wait.ForLog("")),
	)
	if err != nil {
		return nil, fmt.Errorf("starting %s: %w", name, err)
	}

	vb := &Vtbackup{component: component{
		name:     name,
		httpPort: fmt.Sprintf("%d/tcp", tabletMySQLPort),
		cluster:  c,
	}}
	vb.setContainer(ctr)

	c.mu.Lock()
	c.vtbackups = append(c.vtbackups, vb)
	c.mu.Unlock()
	return vb, nil
}

// MySQLAddr returns the host-reachable "host:port" of vtbackup's ephemeral
// mysqld.
func (v *Vtbackup) MySQLAddr(ctx context.Context) (string, error) {
	return v.hostAddr(ctx, fmt.Sprintf("%d/tcp", tabletMySQLPort))
}

// DBAConnParams returns connection parameters for a dba-level connection to
// vtbackup's ephemeral mysqld.
func (v *Vtbackup) DBAConnParams(ctx context.Context, dbName string) (mysql.ConnParams, error) {
	addr, err := v.MySQLAddr(ctx)
	if err != nil {
		return mysql.ConnParams{}, err
	}
	return dbaConnParams(addr, dbName)
}

// Wait blocks until vtbackup exits and returns its full output, erroring when
// it exits non-zero.
func (v *Vtbackup) Wait(ctx context.Context) (string, error) {
	ctr := v.container()
	if ctr == nil {
		return "", fmt.Errorf("%s has no container", v.name)
	}

	waitCtx, cancel := context.WithTimeout(ctx, vtbackupTimeout)
	defer cancel()

	for {
		state, err := ctr.State(waitCtx)
		if err != nil {
			return "", fmt.Errorf("inspecting %s: %w", v.name, err)
		}
		if !state.Running {
			output, err := v.Logs(ctx)
			if err != nil {
				return "", err
			}
			if state.ExitCode != 0 {
				return output, fmt.Errorf("%s exited with code %d", v.name, state.ExitCode)
			}
			return output, nil
		}

		select {
		case <-waitCtx.Done():
			return "", fmt.Errorf("%s did not exit: %w", v.name, waitCtx.Err())
		case <-time.After(defaultPollInterval):
		}
	}
}

// readContainerLog returns a container's full log from the Docker daemon.
func (c *Cluster) readContainerLog(ctx context.Context, ctr testcontainers.Container) (string, error) {
	rc, err := ctr.Logs(ctx)
	if err != nil {
		return "", fmt.Errorf("reading container log: %w", err)
	}
	defer rc.Close()

	content, err := io.ReadAll(rc)
	if err != nil {
		return "", fmt.Errorf("reading container log: %w", err)
	}
	return string(content), nil
}

// ListBackups returns the backups stored for a shard, newest last.
func (c *Cluster) ListBackups(ctx context.Context, keyspace, shard string) ([]string, error) {
	output, err := c.Vtctld().ExecuteCommandWithOutput(ctx, "GetBackups", keyspace+"/"+shard)
	if err != nil {
		return nil, err
	}

	var backups []string
	for line := range strings.SplitSeq(strings.TrimSpace(output), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && line != "[]" {
			backups = append(backups, line)
		}
	}
	return backups, nil
}

// RemoveAllBackups deletes every backup stored for a shard.
func (c *Cluster) RemoveAllBackups(ctx context.Context, keyspace, shard string) error {
	backups, err := c.ListBackups(ctx, keyspace, shard)
	if err != nil {
		return err
	}

	for _, backup := range backups {
		if err := c.Vtctld().ExecuteCommand(ctx, "RemoveBackup", keyspace+"/"+shard, backup); err != nil {
			return fmt.Errorf("removing backup %s: %w", backup, err)
		}
	}
	return nil
}
