// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import "testing"

func TestGetDBName(t *testing.T) {
	wantYes := []string{
		"insert into a.b values(1)",
		"update a.b set c=1",
		"delete from a.b where c=d",
	}
	for _, stmt := range wantYes {
		result, err := GetDBName(stmt)
		if err != nil {
			t.Errorf("error %v on %s", err, stmt)
			continue
		}
		if result != "a" {
			t.Errorf("want a, got %s", result)
		}
	}

	wantNo := []string{
		"insert into a values(1)",
		"update a set c=1",
		"delete from a where c=d",
	}
	for _, stmt := range wantNo {
		result, err := GetDBName(stmt)
		if err != nil {
			t.Errorf("error %v on %s", err, stmt)
			continue
		}
		if result != "" {
			t.Errorf("want '', got %s", result)
		}
	}

	wantErr := []string{
		"select * from a",
		"syntax error",
	}
	for _, stmt := range wantErr {
		_, err := GetDBName(stmt)
		if err == nil {
			t.Errorf("want error, got nil")
		}
	}
}
