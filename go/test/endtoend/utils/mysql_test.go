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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/mysqlctl"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	mysqlParams     mysql.ConnParams
	mysqld          *mysqlctl.Mysqld
	keyspaceName    = "ks"
	cell            = "test"
	schemaSQL       = `create table t1(
		id1 bigint,
		id2 bigint,
		id3 bigint,
		primary key(id1)
	) Engine=InnoDB;`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		var closer func()
		var err error
		mysqlParams, mysqld, closer, err = NewMySQLWithMysqld(clusterInstance.GetAndReservePort(), clusterInstance.Hostname, keyspaceName, schemaSQL)
		if err != nil {
			fmt.Println(err)
			return 1
		}
		defer closer()
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestCreateMySQL(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &mysqlParams)
	require.NoError(t, err)
	AssertMatches(t, conn, "show databases;", `[[VARCHAR("information_schema")] [VARCHAR("ks")] [VARCHAR("mysql")] [VARCHAR("performance_schema")] [VARCHAR("sys")]]`)
	AssertMatches(t, conn, "show tables;", `[[VARCHAR("t1")]]`)
	Exec(t, conn, "insert into t1(id1, id2, id3) values (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	AssertMatches(t, conn, "select * from t1;", `[[INT64(1) INT64(1) INT64(1)] [INT64(2) INT64(2) INT64(2)] [INT64(3) INT64(3) INT64(3)]]`)
}

func TestSetSuperReadOnlyMySQL(t *testing.T) {
	require.NotNil(t, mysqld)
	isSuperReadOnly, _ := mysqld.IsSuperReadOnly()
	assert.False(t, isSuperReadOnly, "super_read_only should be set to False")
	retFunc1, err := mysqld.SetSuperReadOnly(true)
	assert.NotNil(t, retFunc1, "SetSuperReadOnly is supposed to return a defer function")
	assert.NoError(t, err, "SetSuperReadOnly should not have failed")

	isSuperReadOnly, _ = mysqld.IsSuperReadOnly()
	assert.True(t, isSuperReadOnly, "super_read_only should be set to True")
	// if value is already true then retFunc2 will be nil
	retFunc2, err := mysqld.SetSuperReadOnly(true)
	assert.Nil(t, retFunc2, "SetSuperReadOnly is supposed to return a nil function")
	assert.NoError(t, err, "SetSuperReadOnly should not have failed")

	retFunc1()
	isSuperReadOnly, _ = mysqld.IsSuperReadOnly()
	assert.False(t, isSuperReadOnly, "super_read_only should be set to False")
	isReadOnly, _ := mysqld.IsReadOnly()
	assert.True(t, isReadOnly, "read_only should be set to True")

	isSuperReadOnly, _ = mysqld.IsSuperReadOnly()
	assert.False(t, isSuperReadOnly, "super_read_only should be set to False")
	retFunc1, err = mysqld.SetSuperReadOnly(false)
	assert.Nil(t, retFunc1, "SetSuperReadOnly is supposed to return a nil function")
	assert.NoError(t, err, "SetSuperReadOnly should not have failed")

	_, err = mysqld.SetSuperReadOnly(true)
	assert.NoError(t, err)

	isSuperReadOnly, _ = mysqld.IsSuperReadOnly()
	assert.True(t, isSuperReadOnly, "super_read_only should be set to True")
	retFunc1, err = mysqld.SetSuperReadOnly(false)
	assert.NotNil(t, retFunc1, "SetSuperReadOnly is supposed to return a defer function")
	assert.NoError(t, err, "SetSuperReadOnly should not have failed")

	isSuperReadOnly, _ = mysqld.IsSuperReadOnly()
	assert.False(t, isSuperReadOnly, "super_read_only should be set to False")
	// if value is already false then retFunc2 will be nil
	retFunc2, err = mysqld.SetSuperReadOnly(false)
	assert.Nil(t, retFunc2, "SetSuperReadOnly is supposed to return a nil function")
	assert.NoError(t, err, "SetSuperReadOnly should not have failed")

	retFunc1()
	isSuperReadOnly, _ = mysqld.IsSuperReadOnly()
	assert.True(t, isSuperReadOnly, "super_read_only should be set to True")
	isReadOnly, _ = mysqld.IsReadOnly()
	assert.True(t, isReadOnly, "read_only should be set to True")
}
