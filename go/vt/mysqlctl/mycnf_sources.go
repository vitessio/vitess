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

package mysqlctl

import (
	"errors"
	"os"

	"vitess.io/vitess/go/vt/log"
)

// NewMycnfFromSources creates a Mycnf object from multiple sources.
//
// 1. First, create a Mycnf object from NewMycnf.
// 2. Then, merge in any values from mycnf files.
//   - If --mycnf_file is specified, used that. Return an error if the
//     file does not exist or cannot be read.
//   - Otherwise, use a default mycnf file directived from the tablet UID.
//     Return an error if the file exists but cannot be read.
//
// 3. Then, merge in any provided command-line flags.
//
// RegisterFlags should have been called before calling
// this, otherwise we'll panic.
func NewMycnfFromSources(tabletUID uint32, mysqlPort int32) (*Mycnf, error) {
	var cnfs []*Mycnf

	cnfs = append(cnfs, NewMycnf(tabletUID, mysqlPort))

	// Get a Mycnf file.
	if *flagMycnfFile == "" {
		log.Infof("No mycnf_server_id, no mycnf-file specified, using default config for server id %v", tabletUID)
		fcnf, err := NewMycnfFromDefaultFile(tabletUID)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
			// An error here means we couldn't find the tablet-default config file.
		} else {
			cnfs = append(cnfs, fcnf)
		}
	} else {
		path := *flagMycnfFile
		log.Infof("No mycnf_server_id specified, using mycnf-file file: %v", path)
		fcnf, err := NewMycnfFromFile(tabletUID, path)
		if err != nil {
			return nil, err
		}
		cnfs = append(cnfs, fcnf)
	}

	cnfs = append(cnfs, NewMycnfFromFlags(tabletUID))

	return merge(cnfs...), nil
}
