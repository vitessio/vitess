// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func TestSelect(t *testing.T) {
	sqls := []string{
		"select * from user",
		"select * from user where id in (select * from music)",
		"select * from user where id = 1",
		"select * from user where id = :id",
		"select * from user where id in (1, 2)",
		"select * from user where user_id = 1",
		"select * from user_extra where user_id = 1",
		"select * from music where user_id = 1",
		"select * from music where id = 1",
		"select * from music where id in (1, 2)",
		"select * from music where id in (:a, 2)",
		"select * from music where id in ::list",
		"select * from music_extra where music_id = 1",
		"select count(*) from user where id in (1, 2)",
		"select fun(*) from user where id in (1, 2)",
	}
	for _, sql := range sqls {
		sel, err := sqlparser.Parse(sql)
		if err != nil {
			t.Error(err)
			continue
		}
		plan := buildSelectPlan(sel.(*sqlparser.Select), sql)
		fmt.Printf("%+v\n", plan)
	}
}
