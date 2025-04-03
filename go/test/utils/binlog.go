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

package utils

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	ExtraCnf          = "EXTRA_MY_CNF"
	BinlogRowImageCnf = "binlog-row-image.cnf"
)

// SetBinlogRowImageOptions creates a temp cnf file to set binlog_row_image=NOBLOB and
// optionally binlog_row_value_options=PARTIAL_JSON (since it does not exist in 5.7)
// for vreplication unit tests.
// It adds it to the EXTRA_MY_CNF environment variable which appends text from them
// into my.cnf.
func SetBinlogRowImageOptions(mode string, partialJSON bool, cnfDir string) error {
	var newCnfs []string

	// remove any existing extra cnfs for binlog row image
	cnfPath := fmt.Sprintf("%s/%s", cnfDir, BinlogRowImageCnf)
	os.Remove(cnfPath)
	extraCnf := strings.TrimSpace(os.Getenv(ExtraCnf))
	if extraCnf != "" {
		cnfs := strings.Split(extraCnf, ":")
		for _, cnf := range cnfs {
			if !strings.Contains(cnf, BinlogRowImageCnf) {
				newCnfs = append(newCnfs, cnf)
			}
		}
	}

	// If specified add extra cnf for binlog row image, otherwise we will have reverted any previous specification.
	if mode != "" {
		f, err := os.Create(cnfPath)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(f, "\nbinlog_row_image=%s\n", mode)
		if err != nil {
			return err
		}
		if partialJSON {
			if !CIDBPlatformIsMySQL8orLater() {
				return errors.New("partial JSON values are only supported in MySQL 8.0 or later")
			}
			// We're testing partial binlog row images so let's also test partial
			// JSON values in the images.
			_, err = f.WriteString("\nbinlog_row_value_options=PARTIAL_JSON\n")
			if err != nil {
				return err
			}
		}
		err = f.Close()
		if err != nil {
			return err
		}

		newCnfs = append(newCnfs, cnfPath)
	}
	err := os.Setenv(ExtraCnf, strings.Join(newCnfs, ":"))
	if err != nil {
		return err
	}
	return nil
}

// CIDBPlatformIsMySQL8orLater returns true if the CI_DB_PLATFORM environment
// variable is empty -- meaning we're not running in the CI and we assume
// MySQL8.0 or later is used, and you can understand the failures and make
// adjustments as necessary -- or it's set to reflect usage of MySQL 8.0 or
// later. This relies on the current standard values used such as mysql57,
// mysql80, mysql84, etc. This can be used when the CI test behavior needs
// to be altered based on the specific database platform we're testing against.
func CIDBPlatformIsMySQL8orLater() bool {
	dbPlatform := strings.ToLower(os.Getenv("CI_DB_PLATFORM"))
	if dbPlatform == "" {
		// This is for local testing where we don't set the env var via
		// the CI.
		return true
	}
	if strings.HasPrefix(dbPlatform, "mysql") {
		_, v, ok := strings.Cut(dbPlatform, "mysql")
		if ok {
			// We only want the major version.
			version, err := strconv.Atoi(string(v[0]))
			if err == nil && version >= 8 {
				return true
			}
		}
	}
	return false
}
