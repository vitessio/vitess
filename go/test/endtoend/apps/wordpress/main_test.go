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

package wordpress

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"
	"testing"
	"time"

	"database/sql"

	vtenv "vitess.io/vitess/go/vt/env"

	_ "github.com/go-sql-driver/mysql"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "wordpressdb"
	Cell            = "test"

	VSchema = `{
	"sharded": false,
	"tables": {
		"wp_term_relationships":{},
		"wp_comments":{},
		"wp_links":{},
		"wp_options":{},
		"wp_postmeta":{},
		"wp_term_taxonomy":{},
		"wp_usermeta":{},
		"wp_termmeta":{},
		"wp_terms":{},
		"wp_commentmeta":{},
		"wp_posts":{},
		"wp_users":{}
	}
}`
)

func TestMain(m *testing.M) {
	flag.Parse()
	current, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	path := current + "/wordpress.cnf"
	os.Setenv("EXTRA_MY_CNF", path)
	exitCode := func() int {
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: "",
			VSchema:   VSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*keyspace, 1, true)
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		startVanillaMySQL()

		return m.Run()
	}()

	if mysqld != nil {
		fmt.Println("killing mysqld after tests")
		mysqld.Process.Signal(syscall.SIGKILL)
	}

	os.Exit(exitCode)
}

var mysqld *exec.Cmd
var socketFile string

func startVanillaMySQL() {
	handleErr := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	tmpDir, err := ioutil.TempDir("", "vitess_tests")
	handleErr(err)

	vtMysqlRoot, err := vtenv.VtMysqlRoot()
	handleErr(err)

	mysqldPath, err := binaryPath(vtMysqlRoot, "mysqld")
	handleErr(err)

	datadir := fmt.Sprintf("--datadir=%s", tmpDir)
	basedir := "--basedir=" + vtMysqlRoot
	args := []string{
		basedir,
		datadir,
		"--initialize-insecure",
	}

	initDbCmd, err := startCommand(mysqldPath, args)
	handleErr(err)

	err = initDbCmd.Wait()
	handleErr(err)

	tmpPort, err := getFreePort()
	handleErr(err)
	socketFile = tmpDir + "socket_file"
	args = []string{
		basedir,
		datadir,
		fmt.Sprintf("--port=%d", tmpPort),
		"--socket=" + socketFile,
	}

	mysqld, err = startCommand(mysqldPath, args)
	handleErr(err)
	time.Sleep(1 * time.Second) // give mysqld a chance to start listening to the socket before running tests

	planMysql, err := sql.Open("mysql", fmt.Sprintf("root@unix(%s)/", socketFile))
	handleErr(err)
	defer planMysql.Close()
	_, err = planMysql.Exec("create database wordpressdb")
	handleErr(err)
}

func startCommand(mysqldPath string, args []string) (*exec.Cmd, error) {
	cmd := exec.Command(mysqldPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd, cmd.Start()
}

// binaryPath does a limited path lookup for a command,
// searching only within sbin and bin in the given root.
func binaryPath(root, binary string) (string, error) {
	subdirs := []string{"sbin", "bin", "libexec", "scripts"}
	for _, subdir := range subdirs {
		binPath := path.Join(root, subdir, binary)
		if _, err := os.Stat(binPath); err == nil {
			return binPath, nil
		}
	}
	return "", fmt.Errorf("%s not found in any of %s/{%s}",
		binary, root, strings.Join(subdirs, ","))
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
