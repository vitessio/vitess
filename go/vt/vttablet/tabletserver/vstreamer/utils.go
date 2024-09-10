/*
Copyright 2024 The Vitess Authors.

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

package vstreamer

import (
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func GetVReplicationConfig(options *binlogdatapb.VStreamOptions) (*vttablet.VReplicationConfig, error) {
	if options == nil {
		return vttablet.InitVReplicationConfigDefaults(), nil
	}
	config, err := vttablet.NewVReplicationConfig(options.ConfigOverrides)
	if err != nil {
		log.Errorf("Error parsing VReplicationConfig: %v", err)
		return nil, vterrors.Wrapf(err, "failed to parse VReplicationConfig")
	}
	return config, nil
}
