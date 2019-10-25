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

package vitessdriver

import (
	"database/sql/driver"
	"fmt"
	"time"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type converter struct {
	location *time.Location
}

func (cv *converter) ToNative(v sqltypes.Value) (interface{}, error) {
	switch v.Type() {
	case sqltypes.Datetime:
		return DatetimeToNative(v, cv.location)
	case sqltypes.Date:
		return DateToNative(v, cv.location)
	}
	return sqltypes.ToNative(v)
}

func (cv *converter) BuildBindVariable(v interface{}) (*querypb.BindVariable, error) {
	if t, ok := v.(time.Time); ok {
		return sqltypes.ValueBindVariable(NewDatetime(t, cv.location)), nil
	}
	return sqltypes.BuildBindVariable(v)
}

// populateRow populates a row of data using the table's field descriptions.
// The returned types for "dest" include the list from the interface
// specification at https://golang.org/pkg/database/sql/driver/#Value
// and in addition the type "uint64" for unsigned BIGINT MySQL records.
func (cv *converter) populateRow(dest []driver.Value, row []sqltypes.Value) (err error) {
	for i := range dest {
		dest[i], err = cv.ToNative(row[i])
		if err != nil {
			return
		}
	}
	return
}

func (cv *converter) buildBindVars(args []driver.Value) (map[string]*querypb.BindVariable, error) {
	bindVars := make(map[string]*querypb.BindVariable, len(args))
	for i, v := range args {
		bv, err := cv.BuildBindVariable(v)
		if err != nil {
			return nil, err
		}
		bindVars[fmt.Sprintf("v%d", i+1)] = bv
	}
	return bindVars, nil
}

func (cv *converter) bindVarsFromNamedValues(args []driver.NamedValue) (map[string]*querypb.BindVariable, error) {
	bindVars := make(map[string]*querypb.BindVariable, len(args))
	nameUsed := false
	for i, v := range args {
		bv, err := cv.BuildBindVariable(v.Value)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			// Determine if args are based on names or ordinals.
			if v.Name != "" {
				nameUsed = true
			}
		} else {
			// Verify that there's no intermixing.
			if nameUsed && v.Name == "" {
				return nil, errNoIntermixing
			}
			if !nameUsed && v.Name != "" {
				return nil, errNoIntermixing
			}
		}
		if v.Name == "" {
			bindVars[fmt.Sprintf("v%d", i+1)] = bv
		} else {
			if v.Name[0] == ':' || v.Name[0] == '@' {
				bindVars[v.Name[1:]] = bv
			} else {
				bindVars[v.Name] = bv
			}
		}
	}
	return bindVars, nil
}

func newConverter(cfg *Configuration) (c *converter, err error) {
	c = &converter{
		location: time.UTC,
	}
	if cfg.DefaultLocation != "" {
		c.location, err = time.LoadLocation(cfg.DefaultLocation)
	}
	return
}
