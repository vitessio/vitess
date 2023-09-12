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
	"fmt"
	"os"
	"strings"
)

const (
	ExtraCnf          = "EXTRA_MY_CNF"
	BinlogRowImageCnf = "binlog-row-image.cnf"
)

// SetBinlogRowImageMode  creates a temp cnf file to set binlog_row_image to noblob for vreplication unit tests.
// It adds it to the EXTRA_MY_CNF environment variable which appends text from them into my.cnf.
func SetBinlogRowImageMode(mode string, cnfDir string) error {
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
		_, err = f.WriteString(fmt.Sprintf("\nbinlog_row_image=%s\n", mode))
		if err != nil {
			return err
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
