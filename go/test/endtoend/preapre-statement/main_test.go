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

package preparestmt

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"

	_ "github.com/jinzhu/gorm/dialects/mysql"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtgateHost      string
	vtgatePort      uint
	sqlDebug        = false
	hostname        = "localhost"
	keyspaceName    = "test_keyspace"
	dbInfo          = DBInfo{
		KeyspaceName: keyspaceName,
		Username:     "testuser1",
		Password:     "testpassword1",
		Params: []string{
			"charset=utf8",
			"parseTime=True",
			"loc=Local",
		},
	}
	tableName             = "vt_prepare_stmt_test"
	cell                  = "zone1"
	tableACLConfig        = "/table_acl_config.json"
	mysqlAuthServerStatic = "/mysql_auth_server_static.json"
	jsonExample           = `{
    "quiz": {
        "sport": {
            "q1": {
                "question": "Which one is correct team name in NBA?",
                "options": [
                    "New York Bulls",
                    "Los Angeles Kings",
                    "Golden State Warriors",
                    "Huston Rocket"
                ],
                "answer": "Huston Rocket"
            }
        },
        "maths": {
            "q1": {
                "question": "5 + 7 = ?",
                "options": [
                    "10",
                    "11",
                    "12",
                    "13"
                ],
                "answer": "12"
            },
            "q2": {
                "question": "12 - 8 = ?",
                "options": [
                    "1",
                    "2",
                    "3",
                    "4"
                ],
                "answer": "4"
            }
        }
    }
}`
	sqlSchema = `create table ` + tableName + ` (
		id bigint auto_increment,
		msg varchar(64),
		keyspace_id bigint(20) unsigned NOT NULL,
		tinyint_unsigned TINYINT,
		bool_signed BOOL,
		smallint_unsigned SMALLINT,
		mediumint_unsigned MEDIUMINT,
		int_unsigned INT,
		float_unsigned FLOAT(10,2),
		double_unsigned DOUBLE(16,2),
		decimal_unsigned DECIMAL,
		t_date DATE,
		t_datetime DATETIME,
		t_time TIME,
		t_timestamp TIMESTAMP,
		c8 bit(8) DEFAULT NULL,
		c16 bit(16) DEFAULT NULL,
		c24 bit(24) DEFAULT NULL,
		c32 bit(32) DEFAULT NULL,
		c40 bit(40) DEFAULT NULL,
		c48 bit(48) DEFAULT NULL,
		c56 bit(56) DEFAULT NULL,
		c63 bit(63) DEFAULT NULL,
		c64 bit(64) DEFAULT NULL,
		json_col JSON,
		text_col TEXT,
		data longblob,
		primary key (id)
		) Engine=InnoDB`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)

		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// create acl config
		ACLConfig := `{
					"table_groups": [
						{
						   "table_names_or_prefixes": ["vt_prepare_stmt_test", "dual"],
						   "readers": ["vtgate client 1"],
						   "writers": ["vtgate client 1"],
						   "admins": ["vtgate client 1"]
						}
					]
			  }`
		if err := createConfig(tableACLConfig, ACLConfig); err != nil {
			return 1, err
		}

		// create auth server config
		SQLConfig := `{
					"testuser1": {
					  "Password": "testpassword1",
					  "UserData": "vtgate client 1"
					},
					"testuser2": {
					  "Password": "testpassword2",
					  "UserData": "vtgate client 2"
					}
			  }`
		if err := createConfig(mysqlAuthServerStatic, SQLConfig); err != nil {
			return 1, err
		}

		// add extra arguments
		clusterInstance.VtGateExtraArgs = []string{
			"-mysql_auth_server_impl",
			"static",
			"-mysql_server_query_timeout",
			"1s",
			"-mysql_auth_server_static_file",
			clusterInstance.TmpDirectory + mysqlAuthServerStatic,
			"-mysql_server_version",
			"8.0.16-7",
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
			return 1, err
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1, err
		}

		dbInfo.Host = clusterInstance.Hostname
		dbInfo.Port = uint(clusterInstance.VtgateMySQLPort)

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}

}
