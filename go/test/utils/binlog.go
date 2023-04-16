package utils

import (
	"fmt"
	"os"
	"strings"
)

func SetBinlogRowImageMode(mode string, cnfFilePath string) error {
	const ExtraCnf = "EXTRA_MY_CNF"
	const BinlogRowImageCnf = "binlog-row-image.cnf"

	// remove any existing extra cnfs for binlog row image
	extraCnf := os.Getenv(ExtraCnf)
	cnfs := strings.Split(extraCnf, ":")
	var newCnfs []string
	for _, cnf := range cnfs {
		if !strings.Contains(cnf, BinlogRowImageCnf) {
			newCnfs = append(newCnfs, cnf)
		}
	}

	// if specified add extra cnf for binlog row image, otherwise we will have reverted any previous specification
	if mode != "" {
		f, err := os.Create(fmt.Sprintf("%s/%s", cnfFilePath, BinlogRowImageCnf))
		if err != nil {
			return err
		}
		_, err = f.WriteString(fmt.Sprintf("\nbinlog_row_image='%s'\n", mode))
		if err != nil {
			return err
		}
		err = f.Close()
		if err != nil {
			return err
		}

		newCnfs = append(newCnfs, cnfFilePath)
	}
	err := os.Setenv(ExtraCnf, strings.Join(newCnfs, ":"))
	if err != nil {
		return err
	}
	return nil
}
