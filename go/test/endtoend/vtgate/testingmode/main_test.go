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

package testingmode

import (
	"context"
	"database/sql"
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

	"github.com/google/go-cmp/cmp"

	"vitess.io/vitess/go/sqltypes"

	"github.com/stretchr/testify/require"

	vtenv "vitess.io/vitess/go/vt/env"

	_ "github.com/go-sql-driver/mysql"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "ks"
	Cell            = "test"
	SchemaSQL       = `create table t1(
	id1 bigint,
	id2 bigint,
	primary key(id1)
) Engine=InnoDB;

create table t1_id2_idx(
	id2 bigint,
	keyspace_id varbinary(10),
	primary key(id2)
) Engine=InnoDB;

create table vstream_test(
	id bigint,
	val bigint,
	primary key(id)
) Engine=InnoDB;

create table aggr_test(
	id bigint,
	val1 varchar(16),
	val2 bigint,
	primary key(id)
) Engine=InnoDB;

create table t2(
	id3 bigint,
	id4 bigint,
	primary key(id3)
) Engine=InnoDB;

create table t2_id4_idx(
	id bigint not null auto_increment,
	id4 bigint,
	id3 bigint,
	primary key(id),
	key idx_id4(id4)
) Engine=InnoDB;

create table t3(
	id5 bigint,
	id6 bigint,
	id7 bigint,
	primary key(id5)
) Engine=InnoDB;

create table t3_id7_idx(
    id bigint not null auto_increment,
	id7 bigint,
	id6 bigint,
    primary key(id)
) Engine=InnoDB;

create table t4(
	id1 bigint,
	id2 varchar(10),
	primary key(id1)
) ENGINE=InnoDB DEFAULT charset=utf8mb4 COLLATE=utf8mb4_general_ci;

create table t4_id2_idx(
	id2 varchar(10),
	id1 bigint,
	keyspace_id varbinary(50),
    primary key(id2, id1)
) Engine=InnoDB DEFAULT charset=utf8mb4 COLLATE=utf8mb4_general_ci;

create table t5_null_vindex(
	id bigint not null,
	idx varchar(50),
	primary key(id)
) Engine=InnoDB;

create table t6(
	id1 bigint,
	id2 varchar(10),
	primary key(id1)
) Engine=InnoDB;

create table t6_id2_idx(
	id2 varchar(10),
	id1 bigint,
	keyspace_id varbinary(50),
	primary key(id1),
	key(id2)
) Engine=InnoDB;

create table t7_xxhash(
	uid varchar(50),
	phone bigint,
    msg varchar(100),
    primary key(uid)
) Engine=InnoDB;

create table t7_xxhash_idx(
	phone bigint,
	keyspace_id varbinary(50),
	primary key(phone, keyspace_id)
) Engine=InnoDB;

create table t7_fk(
	id bigint,
	t7_uid varchar(50),
    primary key(id),
    CONSTRAINT t7_fk_ibfk_1 foreign key (t7_uid) references t7_xxhash(uid)
    on delete set null on update cascade
) Engine=InnoDB;
`

	VSchema = `
{
  "sharded": true,
  "vindexes": {
    "unicode_loose_xxhash" : {
	  "type": "unicode_loose_xxhash"
    },
    "unicode_loose_md5" : {
	  "type": "unicode_loose_md5"
    },
    "hash": {
      "type": "hash"
    },
    "xxhash": {
      "type": "xxhash"
    },
    "t1_id2_vdx": {
      "type": "consistent_lookup_unique",
      "params": {
        "table": "t1_id2_idx",
        "from": "id2",
        "to": "keyspace_id"
      },
      "owner": "t1"
    },
    "t2_id4_idx": {
      "type": "lookup_hash",
      "params": {
        "table": "t2_id4_idx",
        "from": "id4",
        "to": "id3",
        "autocommit": "true"
      },
      "owner": "t2"
    },
    "t3_id7_vdx": {
      "type": "lookup_hash",
      "params": {
        "table": "t3_id7_idx",
        "from": "id7",
        "to": "id6"
      },
      "owner": "t3"
    },
    "t4_id2_vdx": {
      "type": "consistent_lookup",
      "params": {
        "table": "t4_id2_idx",
        "from": "id2,id1",
        "to": "keyspace_id"
      },
      "owner": "t4"
    },
    "t6_id2_vdx": {
      "type": "consistent_lookup",
      "params": {
        "table": "t6_id2_idx",
        "from": "id2,id1",
        "to": "keyspace_id",
        "ignore_nulls": "true"
      },
      "owner": "t6"
    },
    "t7_xxhash_vdx": {
      "type": "consistent_lookup",
      "params": {
        "table": "t7_xxhash_idx",
        "from": "phone",
        "to": "keyspace_id",
        "ignore_nulls": "true"
      },
      "owner": "t7_xxhash"
    }
  },
  "tables": {
    "t1": {
      "column_vindexes": [
        {
          "column": "id1",
          "name": "hash"
        },
        {
          "column": "id2",
          "name": "t1_id2_vdx"
        }
      ]
    },
    "t1_id2_idx": {
      "column_vindexes": [
        {
          "column": "id2",
          "name": "hash"
        }
      ]
    },
    "t2": {
      "column_vindexes": [
        {
          "column": "id3",
          "name": "hash"
        },
        {
          "column": "id4",
          "name": "t2_id4_idx"
        }
      ]
    },
    "t2_id4_idx": {
      "column_vindexes": [
        {
          "column": "id4",
          "name": "hash"
        }
      ]
    },
    "t3": {
      "column_vindexes": [
        {
          "column": "id6",
          "name": "hash"
        },
        {
          "column": "id7",
          "name": "t3_id7_vdx"
        }
      ]
    },
    "t3_id7_idx": {
      "column_vindexes": [
        {
          "column": "id7",
          "name": "hash"
        }
      ]
    },
	"t4": {
      "column_vindexes": [
        {
          "column": "id1",
          "name": "hash"
        },
        {
          "columns": ["id2", "id1"],
          "name": "t4_id2_vdx"
        }
      ]
    },
    "t4_id2_idx": {
      "column_vindexes": [
        {
          "column": "id2",
          "name": "unicode_loose_md5"
        }
      ]
    },
	"t6": {
      "column_vindexes": [
        {
          "column": "id1",
          "name": "hash"
        },
        {
          "columns": ["id2", "id1"],
          "name": "t6_id2_vdx"
        }
      ]
    },
    "t6_id2_idx": {
      "column_vindexes": [
        {
          "column": "id2",
          "name": "xxhash"
        }
      ]
    },
	"t5_null_vindex": {
      "column_vindexes": [
        {
          "column": "idx",
          "name": "xxhash"
        }
      ]
    },
    "vstream_test": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "hash"
        }
      ]
    },
    "aggr_test": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "hash"
        }
      ],
      "columns": [
        {
          "name": "val1",
          "type": "VARCHAR"
        }
      ]
    },
	"t7_xxhash": {
      "column_vindexes": [
        {
          "column": "uid",
          "name": "unicode_loose_xxhash"
        },
        {
          "column": "phone",
          "name": "t7_xxhash_vdx"
        }
      ]
    },
    "t7_xxhash_idx": {
      "column_vindexes": [
        {
          "column": "phone",
          "name": "unicode_loose_xxhash"
        }
      ]
    },
	"t7_fk": {
      "column_vindexes": [
        {
          "column": "t7_uid",
          "name": "unicode_loose_xxhash"
        }
      ]
    }
  }
}`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

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
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, true)
		if err != nil {
			return 1
		}

		err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildVSchemaGraph")
		if err != nil {
			return 1
		}

		// Start vtgate
		vtgateInstance := *clusterInstance.NewVtgateInstance()
		clusterInstance.VtgateProcess = vtgateInstance
		log.Infof("Starting vtgate on port %d", vtgateInstance.Port)
		log.Infof("Vtgate started, connect to mysql using : mysql -h 127.0.0.1 -P %d", clusterInstance.VtgateMySQLPort)

		startVanillaMySQL()

		initializeMysql()

		fmt.Println("initialization of vanilla mysql complete")

		err = clusterInstance.VtgateProcess.SetupWithTestingMysql(tmpPort)
		if err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		fmt.Println("starting the test case")

		defer func() {
			if mysqld != nil {
				fmt.Println("killing mysqld after tests")
				mysqld.Process.Signal(syscall.SIGKILL)
			}
		}()

		return m.Run()
	}()

	os.Exit(exitCode)
}

func TestTestingCode(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	err = conn.WriteComInitDB(KeyspaceName)
	require.NoError(t, err)
	data, err := conn.ReadPacket()
	require.Equal(t, mysql.OKPacket, int(data[0]))
	require.NoError(t, err)

	assertMatches(t, conn, "select 1+2", "[[INT64(3)]]")
	assertMatches(t, conn, "show tables", `[[VARCHAR("aggr_test")] [VARCHAR("t1")] [VARCHAR("t1_id2_idx")] [VARCHAR("t2")] [VARCHAR("t2_id4_idx")] [VARCHAR("t3")] [VARCHAR("t3_id7_idx")] [VARCHAR("t4")] [VARCHAR("t4_id2_idx")] [VARCHAR("t5_null_vindex")] [VARCHAR("t6")] [VARCHAR("t6_id2_idx")] [VARCHAR("t7_fk")] [VARCHAR("t7_xxhash")] [VARCHAR("t7_xxhash_idx")] [VARCHAR("vstream_test")]]`)
}

func assertMatches(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := execute(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	diff := cmp.Diff(expected, got)
	if diff != "" {
		t.Errorf("Query: %s (-want +got):\n%s", query, diff)
	}
}

func execute(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err, "for query: "+query)
	return qr
}

var mysqld *exec.Cmd
var tmpPort int

func initializeMysql() {
	handleErr := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("Creating the keyspace in vanilla mysql")
	planMysql, err := sql.Open("mysql", fmt.Sprintf("root:@tcp(localhost:%d)/", tmpPort))
	handleErr(err)
	_, err = planMysql.Exec(fmt.Sprintf("create database %s", KeyspaceName))
	handleErr(err)
	planMysql.Close()

	fmt.Println("Starting new connection to the keyspace")
	planMysql, err = sql.Open("mysql", fmt.Sprintf("root:@tcp(localhost:%d)/%s", tmpPort, KeyspaceName))
	handleErr(err)
	defer planMysql.Close()

	fmt.Println("running the schema for the given keyspace on vanilla mysql")

	res := strings.Split(SchemaSQL, ";")
	for _, val := range res {
		val = strings.TrimSpace(val)
		if val == "" {
			continue
		}
		fmt.Println(val)
		_, err = planMysql.Exec(val)
		handleErr(err)
	}
}

func startVanillaMySQL() {
	handleErr := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	tmpDir, err := ioutil.TempDir("", "vtgate_testing_mode")
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

	tmpPort, err = getFreePort()
	handleErr(err)
	//socketFile := tmpDir + "/socket_file"
	args = []string{
		basedir,
		datadir,
		fmt.Sprintf("--port=%d", tmpPort),
		//"--socket=" + socketFile,
	}

	mysqld, err = startCommand(mysqldPath, args)
	handleErr(err)
	time.Sleep(1 * time.Second)
}

func startCommand(mysqldPath string, args []string) (*exec.Cmd, error) {
	cmd := exec.Command(mysqldPath, args...)
	fmt.Println(cmd.String())
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
