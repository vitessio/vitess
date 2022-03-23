/*
Copyright 2022 The Vitess Authors.

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

package utils

import (
	"context"
	"fmt"
	"os"
	"path"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/mysqlctl"
)

func NewMySQL(cluster *cluster.LocalProcessCluster, ksName string) (mysql.ConnParams, func(), error) {
	mysqlDir, err := createMySQLDir()
	if err != nil {
		return mysql.ConnParams{}, nil, err
	}

	initMySQLFile, err := createInitSQLFile(mysqlDir, ksName)
	if err != nil {
		return mysql.ConnParams{}, nil, err
	}

	mysqlPort := cluster.GetAndReservePort()
	mysqld, mycnf, err := mysqlctl.CreateMysqldAndMycnf(0, "", int32(mysqlPort))
	if err != nil {
		return mysql.ConnParams{}, nil, err
	}

	err = initMysqld(mysqld, mycnf, initMySQLFile)
	if err != nil {
		return mysql.ConnParams{}, nil, err
	}

	params := mysql.ConnParams{
		UnixSocket: mycnf.SocketFile,
		Host:       cluster.Hostname,
		Uname:      "root",
		DbName:     ksName,
	}
	return params, func() {
		ctx := context.Background()
		_ = mysqld.Teardown(ctx, mycnf, true)
	}, nil
}

func createMySQLDir() (string, error) {
	mysqlDir := mysqlctl.TabletDir(0)
	err := os.Mkdir(mysqlDir, 0700)
	if err != nil {
		return "", err
	}
	return mysqlDir, nil
}

func createInitSQLFile(mysqlDir, ksName string) (string, error) {
	initSQLFile := path.Join(mysqlDir, "init.sql")
	f, err := os.Create(initSQLFile)
	if err != nil {
		return "", err
	}
	defer f.Close()

	_, err = f.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", ksName))
	if err != nil {
		return "", err
	}
	return initSQLFile, nil
}

func initMysqld(mysqld *mysqlctl.Mysqld, mycnf *mysqlctl.Mycnf, initSQLFile string) error {
	f, err := os.CreateTemp(path.Dir(mycnf.Path), "my.cnf")
	if err != nil {
		return err
	}
	f.Close()

	ctx := context.Background()
	err = mysqld.Init(ctx, mycnf, initSQLFile)
	if err != nil {
		return err
	}
	return nil
}
