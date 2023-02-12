/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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

package logic

// This file holds wrappers around routines to check if global
// recovery is disabled or not.
//
// This is determined by looking in the table
// vtorc.global_recovery_disable for a value 1.  Note: for
// recoveries to actually happen this must be configured explicitly
// in vtorc.conf.json. This setting is an emergency brake
// to quickly be able to prevent recoveries happening in some large
// outage type situation.  Maybe this value should be cached etc
// but we won't be doing that many recoveries at once so the load
// on this table is expected to be very low. It should be fine to
// go to the database each time.

import (
	"fmt"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtorc/db"
)

// IsRecoveryDisabled returns true if Recoveries are disabled globally
func IsRecoveryDisabled() (disabled bool, err error) {
	query := `
		SELECT
			COUNT(*) as mycount
		FROM
			global_recovery_disable
		WHERE
			disable_recovery=?
		`
	err = db.QueryVTOrc(query, sqlutils.Args(1), func(m sqlutils.RowMap) error {
		mycount := m.GetInt("mycount")
		disabled = (mycount > 0)
		return nil
	})
	if err != nil {
		errMsg := fmt.Sprintf("recovery.IsRecoveryDisabled(): %v", err)
		log.Errorf(errMsg)
		err = fmt.Errorf(errMsg)
	}
	return disabled, err
}

// DisableRecovery ensures recoveries are disabled globally
func DisableRecovery() error {
	_, err := db.ExecVTOrc(`
		INSERT IGNORE INTO global_recovery_disable
			(disable_recovery)
		VALUES  (1)
	`,
	)
	return err
}

// EnableRecovery ensures recoveries are enabled globally
func EnableRecovery() error {
	// The "WHERE" clause is just to avoid full-scan reports by monitoring tools
	_, err := db.ExecVTOrc(`
		DELETE FROM global_recovery_disable WHERE disable_recovery >= 0
	`,
	)
	return err
}
