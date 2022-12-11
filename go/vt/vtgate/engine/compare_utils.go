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

package engine

import (
	"encoding/json"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func printMismatch(leftResult, rightResult *sqltypes.Result, leftPrimitive, rightPrimitive Primitive, leftName, rightName string) {
	log.Errorf("Results of %s and %s are not equal. Displaying diff.", rightName, leftName)

	// get right plan and print it
	rightplan := &Plan{
		Instructions: rightPrimitive,
	}
	rightJSON, _ := json.MarshalIndent(rightplan, "", "  ")
	log.Errorf("%s's plan:\n%s", rightName, string(rightJSON))

	// get left's plan and print it
	leftplan := &Plan{
		Instructions: leftPrimitive,
	}
	leftJSON, _ := json.MarshalIndent(leftplan, "", "  ")
	log.Errorf("%s's plan:\n%s", leftName, string(leftJSON))

	log.Errorf("%s's results:\n", rightName)
	log.Errorf("\t[rows affected: %d]\n", rightResult.RowsAffected)
	for _, row := range rightResult.Rows {
		log.Errorf("\t%s", row)
	}
	log.Errorf("%s's results:\n", leftName)
	log.Errorf("\t[rows affected: %d]\n", leftResult.RowsAffected)
	for _, row := range leftResult.Rows {
		log.Errorf("\t%s", row)
	}
	log.Error("End of diff.")
}

// CompareErrors compares the two errors, and if they don't match, produces an error
func CompareErrors(leftErr, rightErr error, leftName, rightName string) error {
	if leftErr != nil && rightErr != nil {
		if leftErr.Error() == rightErr.Error() {
			return rightErr
		}
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s and %s failed with different errors: %s: [%s], %s: [%s]", leftName, rightName, leftErr.Error(), rightErr.Error(), leftName, rightName)
	}
	if leftErr == nil && rightErr != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s failed while %s did not: %s", rightName, rightErr.Error(), leftName)
	}
	if leftErr != nil && rightErr == nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s failed while %s did not: %s", leftName, leftErr.Error(), rightName)
	}
	return nil
}
