/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/
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

package vrepl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vtenv"
)

func TestRemovedForeignKeyNames(t *testing.T) {

	tcases := []struct {
		before string
		after  string
		names  []string
	}{
		{
			before: "create table t (id int primary key)",
			after:  "create table t (id2 int primary key, i int)",
		},
		{
			before: "create table t (id int primary key)",
			after:  "create table t2 (id2 int primary key, i int)",
		},
		{
			before: "create table t (id int primary key, i int, constraint f foreign key (i) references parent (id) on delete cascade)",
			after:  "create table t (id int primary key, i int, constraint f foreign key (i) references parent (id) on delete cascade)",
		},
		{
			before: "create table t (id int primary key, i int, constraint f1 foreign key (i) references parent (id) on delete cascade)",
			after:  "create table t (id int primary key, i int, constraint f2 foreign key (i) references parent (id) on delete cascade)",
		},
		{
			before: "create table t (id int primary key, i int, constraint f foreign key (i) references parent (id) on delete cascade)",
			after:  "create table t (id int primary key, i int)",
			names:  []string{"f"},
		},
		{
			before: "create table t (id int primary key, i int, i2 int, constraint f1 foreign key (i) references parent (id) on delete cascade, constraint fi2 foreign key (i2) references parent (id) on delete cascade)",
			after:  "create table t (id int primary key, i int, i2 int, constraint f2 foreign key (i) references parent (id) on delete cascade)",
			names:  []string{"fi2"},
		},
		{
			before: "create table t1 (id int primary key, i int, constraint `check1` CHECK ((`i` < 5)))",
			after:  "create table t2 (id int primary key, i int)",
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.before, func(t *testing.T) {
			names, err := RemovedForeignKeyNames(vtenv.NewTestEnv(), tcase.before, tcase.after)
			assert.NoError(t, err)
			assert.Equal(t, tcase.names, names)
		})
	}
}
