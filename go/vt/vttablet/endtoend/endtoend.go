// Package endtoend is a test-only package. It runs various
// end-to-end tests on tabletserver.
package endtoend

import (
	"encoding/json"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
)

func prettyPrint(qr sqltypes.Result) string {
	out, err := json.Marshal(qr)
	if err != nil {
		log.Errorf("Could not marshal result to json for %#v", qr)
		return fmt.Sprintf("%#v", qr)
	}
	return string(out)
}
