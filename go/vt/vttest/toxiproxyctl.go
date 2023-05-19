/*
Copyright 2023 The Vitess Authors.

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

package vttest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	// dbaUser is the user who can access through toxiproxy to the MySQL server.
	dbaUser = "vt_dba_tcp"
)

// Toxiproxyctl implements MySQLManager through toxiproxy-server tool.
// It is used to simulate network failures and test the behavior of vttablet.
type Toxiproxyctl struct {
	binary   string
	apiPort  int
	port     int
	mysqlctl *Mysqlctl
	logPath  string

	cmd    *exec.Cmd
	cancel context.CancelFunc

	proxy *toxiproxy.Proxy
}

// NewToxiproxyctl creates a new Toxiproxyctl instance.
//
// apiPort is the port number of a toxiproxy server. It is used to create a proxy by the toxiproxy client.
// mysqlPort is the port number of a mysql server running behind the proxy.
func NewToxiproxyctl(binary string, apiPort, mysqlPort int, mysqlctl *Mysqlctl, logPath string) (*Toxiproxyctl, error) {
	// proxyPort is used as the port number of a mysql proxy.
	proxyPort := mysqlctl.Port
	mysqlctl.Port = mysqlPort

	// The original initFile does not have any users who can access through TCP/IP connection.
	// Here we update the init file to create the user.
	// We're using IPv6 localhost because that's what toxiproxy uses by default.
	initDb, _ := os.ReadFile(mysqlctl.InitFile)
	createUserCmd := fmt.Sprintf(`
		# Admin user for TCP/IP connection with all privileges.
		CREATE USER '%s'@'::1';
		GRANT ALL ON *.* TO '%s'@'::1';
		GRANT GRANT OPTION ON *.* TO '%s'@'::1';
	`, dbaUser, dbaUser, dbaUser)
	sql, err := getInitDBSQL(string(initDb), createUserCmd)
	if err != nil {
		return nil, vterrors.Wrap(err, "failed to get a modified init db sql")
	}
	newInitFile := path.Join(mysqlctl.Directory, "init_db_toxiproxyctl.sql")
	err = os.WriteFile(newInitFile, []byte(sql), 0600)
	if err != nil {
		return nil, vterrors.Wrap(err, "failed to write a modified init db file")
	}
	mysqlctl.InitFile = newInitFile

	return &Toxiproxyctl{
		binary:   binary,
		apiPort:  apiPort,
		port:     proxyPort,
		mysqlctl: mysqlctl,
		logPath:  logPath,
	}, nil
}

// Setup spawns a new mysqld service and a new toxiproxy service.
// These services are kept running in the background until TearDown() is called.
func (ctl *Toxiproxyctl) Setup() error {
	err := ctl.mysqlctl.Setup()
	if err != nil {
		return vterrors.Wrap(err, "failed to setup mysqlctl")
	}

	return ctl.run()
}

// Start spawns a new mysqld service and a new toxiproxy service.
// These services are kept running in the background until TearDown() is called.
func (ctl *Toxiproxyctl) Start() error {
	err := ctl.mysqlctl.Start()
	if err != nil {
		return vterrors.Wrap(err, "failed to start mysqlctl")
	}

	return ctl.run()
}

func (ctl *Toxiproxyctl) run() error {
	ctx, cancel := context.WithCancel(context.Background())
	ctl.cancel = cancel

	cmd := exec.CommandContext(ctx,
		ctl.binary,
		"-port", fmt.Sprintf("%d", ctl.apiPort),
	)
	cmd.Env = append(cmd.Env, os.Environ()...)

	logFile, err := os.Create(ctl.logPath)
	if err != nil {
		return vterrors.Wrap(err, "failed to create log file")
	}
	defer logFile.Close()
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	err = cmd.Start()
	if err != nil {
		return vterrors.Wrapf(err, "failed to start toxiproxy")
	}
	log.Infof("Toxiproxy starting on port: %d, logFile: %s", ctl.apiPort, ctl.logPath)
	ctl.cmd = cmd

	attempt := 0
	for {
		// Wait for toxiproxy to start
		time.Sleep(1 * time.Second)

		toxiClient := toxiproxy.NewClient("localhost:" + fmt.Sprintf("%d", ctl.apiPort))
		proxy, err := toxiClient.CreateProxy(
			"mysql",
			"localhost:"+fmt.Sprintf("%d", ctl.port),
			"localhost:"+fmt.Sprintf("%d", ctl.mysqlctl.Port),
		)
		if err == nil {
			ctl.proxy = proxy
			break
		}

		attempt++
		if attempt > 9 {
			return vterrors.Wrap(err, "failed to create proxy")
		}
	}
	return nil
}

// TearDown shutdowns the running mysqld service and toxiproxy service.
func (ctl *Toxiproxyctl) TearDown() error {
	err := ctl.mysqlctl.TearDown()
	if err != nil {
		return vterrors.Wrap(err, "failed to teardown mysqlctl")
	}

	if ctl.cancel != nil {
		ctl.cancel()
	}

	if ctl.cmd != nil {
		return vterrors.Wrap(ctl.cmd.Wait(), "failed to wait for toxiproxy")
	}
	return nil
}

// Auth returns the username/password tuple required to log in to mysqld.
func (ctl *Toxiproxyctl) Auth() (string, string) {
	return dbaUser, ""
}

// Address returns the hostname/tcp port pair required to connect to mysqld.
func (ctl *Toxiproxyctl) Address() (string, int) {
	return "", ctl.port
}

// UnixSocket returns the path to the local Unix socket required to connect to mysqld.
func (ctl *Toxiproxyctl) UnixSocket() string {
	// Toxiproxy does not support Unix sockets
	return ""
}

// TabletDir returns the path where data for this Tablet would be stored.
func (ctl *Toxiproxyctl) TabletDir() string {
	return ctl.mysqlctl.TabletDir()
}

// Params returns the mysql.ConnParams required to connect directly to mysqld
// using Vitess' mysql client.
func (ctl *Toxiproxyctl) Params(dbname string) mysql.ConnParams {
	params := ctl.mysqlctl.Params(dbname)

	params.UnixSocket = ""
	params.Host = "localhost"
	params.Port = ctl.port
	params.Uname = dbaUser
	return params
}

// AddTimeoutToxic adds a timeout toxic to the toxiproxy service.
func (ctl *Toxiproxyctl) AddTimeoutToxic() error {
	log.Info("Adding timeout toxic")
	_, err := ctl.proxy.AddToxic("my-timeout", "timeout", "", 1, toxiproxy.Attributes{
		"timeout": 0,
	})
	return err
}

// UpdateTimeoutToxicity updates the toxicity of the timeout toxic.
func (ctl *Toxiproxyctl) UpdateTimeoutToxicity(toxicity float32) error {
	log.Infof("Updating timeout toxicity to %f", toxicity)
	_, err := ctl.proxy.UpdateToxic("my-timeout", toxicity, toxiproxy.Attributes{
		"timeout": 0,
	})
	return err
}

// RemoveTimeoutToxic removes the timeout toxic from the toxiproxy service.
func (ctl *Toxiproxyctl) RemoveTimeoutToxic() error {
	log.Info("Removing timeout toxic")
	return ctl.proxy.RemoveToxic("my-timeout")
}

// getInitDBSQL is a helper function that retrieves the modified contents of the init_db.sql file with custom SQL statements.
// We avoid using vitess.io/vitess/go/test/endtoend/utils.GetInitDBSQL as importing this package adds unnecessary flags to vttestserver.
func getInitDBSQL(initDBSQL string, customSQL string) (string, error) {
	splitString := strings.Split(initDBSQL, "# {{custom_sql}}")
	if len(splitString) != 2 {
		return "", fmt.Errorf("missing `# {{custom_sql}}` in init_db.sql file")
	}
	var builder strings.Builder
	builder.WriteString(splitString[0])
	builder.WriteString(customSQL)
	builder.WriteString(splitString[1])

	return builder.String(), nil
}
