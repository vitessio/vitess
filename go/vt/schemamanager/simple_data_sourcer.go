// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import "strings"

// SimpleDataSourcer is really simple
type SimpleDataSourcer struct {
	sqls []string
}

// NewSimepleDataSourcer creates a new SimpleDataSourcer instance
func NewSimepleDataSourcer(sqlStr string) *SimpleDataSourcer {
	result := SimpleDataSourcer{}
	for _, sql := range strings.Split(sqlStr, ";") {
		s := strings.TrimSpace(sql)
		if s != "" {
			result.sqls = append(result.sqls, s)
		}
	}
	return &result
}

// Open is a no-op
func (ds *SimpleDataSourcer) Open() error {
	return nil
}

// Read reads schema changes
func (ds *SimpleDataSourcer) Read() ([]string, error) {
	return ds.sqls, nil
}

// Close is a no-op
func (ds *SimpleDataSourcer) Close() error {
	return nil
}

var _ DataSourcer = (*SimpleDataSourcer)(nil)
