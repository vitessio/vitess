package utils

import (
	"fmt"
	"os"
	"strings"
)

func SetBinlogRowImageMode(mode string, cnfDir string) error {
	const ExtraCnf = "EXTRA_MY_CNF"
	const BinlogRowImageCnf = "binlog-row-image.cnf"
	var newCnfs []string

	// remove any existing extra cnfs for binlog row image
	extraCnf := strings.TrimSpace(os.Getenv(ExtraCnf))
	if extraCnf != "" {
		cnfs := strings.Split(extraCnf, ":")
		for _, cnf := range cnfs {
			if !strings.Contains(cnf, BinlogRowImageCnf) {
				newCnfs = append(newCnfs, cnf)
			}
		}
	}

	// if specified add extra cnf for binlog row image, otherwise we will have reverted any previous specification
	if mode != "" {
		cnfPath := fmt.Sprintf("%s/%s", cnfDir, BinlogRowImageCnf)
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
