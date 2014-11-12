// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"testing"

	"github.com/youtube/vitess/go/vt/logutil"
)

func testMakeSplitCreateTableSql(t *testing.T, testCase, schema, strategyStr, expectedCreate, expectedAlter, expectedError string) {
	logger := logutil.NewMemoryLogger()
	strategy, err := NewSplitStrategy(logger, strategyStr)
	if err != nil {
		t.Fatalf("%v: got strategy parsing error: %v", testCase, err)
	}
	create, alter, err := MakeSplitCreateTableSql(logger, schema, "DBNAME", "TABLENAME", strategy)
	if expectedError != "" {
		if err == nil || err.Error() != expectedError {
			t.Fatalf("%v: got '%v' but was expecting error '%v'", testCase, err, expectedError)
		}
	}
	if err != nil {
		t.Fatalf("%v: expected no error but got: %v", testCase, err)
	}
	if create != expectedCreate {
		t.Errorf("%v: create mismatch: got:\n%vexpected:\n%v", testCase, create, expectedCreate)
	}
	if alter != expectedAlter {
		t.Errorf("%v: alter mismatch: got:\n%vexpected:\n%v", testCase, alter, expectedAlter)
	}
}

func TestMakeSplitCreateTableSql(t *testing.T) {
	testMakeSplitCreateTableSql(t, "simple table no index",
		"CREATE TABLE `TABLENAME` (\n"+
			"  `id` bigint(2) NOT NULL,\n"+
			"  `msg` varchar(64) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"",
		"CREATE TABLE `DBNAME`.`TABLENAME` (\n"+
			"  `id` bigint(2) NOT NULL,\n"+
			"  `msg` varchar(64) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"", "")

	testMakeSplitCreateTableSql(t, "simple table primary key",
		"CREATE TABLE `TABLENAME` (\n"+
			"  `id` bigint(2) NOT NULL,\n"+
			"  `msg` varchar(64) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"",
		"CREATE TABLE `DBNAME`.`TABLENAME` (\n"+
			"  `id` bigint(2) NOT NULL,\n"+
			"  `msg` varchar(64) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"", "")

	testMakeSplitCreateTableSql(t, "simple table delay primary key",
		"CREATE TABLE `TABLENAME` (\n"+
			"  `id` bigint(2) NOT NULL,\n"+
			"  `msg` varchar(64) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"-delay_primary_key",
		"CREATE TABLE `DBNAME`.`TABLENAME` (\n"+
			"  `id` bigint(2) NOT NULL,\n"+
			"  `msg` varchar(64) DEFAULT NULL\n"+
			"\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"ALTER TABLE `DBNAME`.`TABLENAME` ADD   PRIMARY KEY (`id`)", "")

	testMakeSplitCreateTableSql(t, "simple table primary key auto increment",
		"CREATE TABLE `TABLENAME` (\n"+
			"  `id` bigint(2) NOT NULL AUTO_INCREMENT,\n"+
			"  `msg` varchar(64) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"",
		"CREATE TABLE `DBNAME`.`TABLENAME` (\n"+
			"  `id` bigint(2) NOT NULL AUTO_INCREMENT,\n"+
			"  `msg` varchar(64) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"", "")

	testMakeSplitCreateTableSql(t, "simple table primary key skip auto increment",
		"CREATE TABLE `TABLENAME` (\n"+
			"  `id` bigint(2) NOT NULL AUTO_INCREMENT,\n"+
			"  `msg` varchar(64) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"-skip_auto_increment=TABLENAME",
		"CREATE TABLE `DBNAME`.`TABLENAME` (\n"+
			"  `id` bigint(2) NOT NULL,\n"+
			"  `msg` varchar(64) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"", "")

	testMakeSplitCreateTableSql(t, "simple table primary key delay auto increment",
		"CREATE TABLE `TABLENAME` (\n"+
			"  `id` bigint(2) NOT NULL AUTO_INCREMENT,\n"+
			"  `msg` varchar(64) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"-delay_auto_increment",
		"CREATE TABLE `DBNAME`.`TABLENAME` (\n"+
			"  `id` bigint(2) NOT NULL,\n"+
			"  `msg` varchar(64) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8",
		"ALTER TABLE `DBNAME`.`TABLENAME` MODIFY   `id` bigint(2) NOT NULL AUTO_INCREMENT", "")
}
