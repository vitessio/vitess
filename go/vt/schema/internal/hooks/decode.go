/*
Copyright 2023 The Vitess Authors.

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

// Package hooks provides internal mapstructure decode hooks used in schema.FromJSON
// to support backwards compatibility with the pre-protobuf message json structure.
//
// Do not use these; they are already deprecated and will be removed in v17 along
// with the older json format.
package hooks

import (
	"fmt"
	"reflect"
	"strings"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

var status tabletmanagerdatapb.OnlineDDL_Status

func DecodeStatus(from, to reflect.Type, data any) (any, error) {
	if to != reflect.TypeOf(status) {
		return data, nil
	}

	var iVal int32
	switch from.Kind() {
	case reflect.String:
		data := data.(string)
		name := strings.ToUpper(data)

		// duplication of schema.ParseOnlineDDLStatus to avoid import cycle.
		val, ok := tabletmanagerdatapb.OnlineDDL_Status_value[name]
		if !ok {
			return nil, fmt.Errorf("unknown enum name for OnlineDDL_Status: %s", data)
		}

		if name != data {
			// TODO: deprecation warning here
		}

		return val, nil
	case reflect.Int:
		iVal = int32(data.(int))
	case reflect.Int32:
		iVal = data.(int32)
	case reflect.Int64:
		iVal = int32(data.(int64))
	case reflect.Float32:
		iVal = int32(data.(float32))
	case reflect.Float64:
		iVal = int32(data.(float64))
	default:
		return nil, fmt.Errorf("invalid type %s for OnlineDDL_Status enum", from.Kind())
	}

	if _, ok := tabletmanagerdatapb.OnlineDDL_Status_name[iVal]; !ok {
		return nil, fmt.Errorf("unknown enum value for OnlineDDL_Status: %d", iVal)
	}

	return tabletmanagerdatapb.OnlineDDL_Status(iVal), nil
}

var strategy tabletmanagerdatapb.OnlineDDL_Strategy

func DecodeStrategy(from, to reflect.Type, data any) (any, error) {
	if to != reflect.TypeOf(strategy) {
		return data, nil
	}

	var iVal int32
	switch from.Kind() {
	case reflect.String:
		data := data.(string)
		name := strings.ToUpper(strings.ReplaceAll(data, "-", ""))

		// duplication of schema.ParseOnlineDDLStrategyName to avoid import cycle.
		val, ok := tabletmanagerdatapb.OnlineDDL_Strategy_value[name]
		if !ok {
			return nil, fmt.Errorf("unknown enum name for OnlineDDL_Strategy: %s", data)
		}

		if name != data {
			// TODO: deprecation warning here
		}

		return val, nil
	case reflect.Int:
		iVal = int32(data.(int))
	case reflect.Int32:
		iVal = data.(int32)
	case reflect.Int64:
		iVal = int32(data.(int64))
	case reflect.Float32:
		iVal = int32(data.(float32))
	case reflect.Float64:
		iVal = int32(data.(float64))
	default:
		return nil, fmt.Errorf("invalid type %s for OnlineDDL_Strategy enum", from.Kind())
	}

	if _, ok := tabletmanagerdatapb.OnlineDDL_Strategy_name[iVal]; !ok {
		return nil, fmt.Errorf("unknown enum value for OnlineDDL_Strategy: %d", iVal)
	}

	return tabletmanagerdatapb.OnlineDDL_Strategy(iVal), nil
}
