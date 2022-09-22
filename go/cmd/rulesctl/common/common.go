package common

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	vtfcr "vitess.io/vitess/go/vt/vttablet/customrule/filecustomrule"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

func GetRules(path string) *rules.Rules {
	rules, err := vtfcr.ParseRules(path)
	if err != nil {
		log.Fatalf("Failure attempting to parse rules: %v", err)
	}
	return rules
}

func MustPrintJSON(obj any) {
	enc, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		log.Fatalf("Unable to marshal object: %v", err)
	}
	fmt.Printf("%v\n", string(enc))
}

func MustWriteJSON(obj any, path string) {
	enc, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		log.Fatalf("Unable to marshal object: %v", err)
	}

	err = os.WriteFile(path, enc, 0400)
	if err != nil {
		log.Fatalf("Unable to save new JSON: %v", err)
	}
}
