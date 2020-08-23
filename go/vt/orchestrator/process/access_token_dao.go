/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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

package process

import (
	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/orchestrator/util"
)

// GenerateAccessToken attempts to generate a new access token and returns the public
// part of the token
func GenerateAccessToken(owner string) (publicToken string, err error) {
	publicToken = util.NewToken().Hash
	secretToken := util.NewToken().Hash

	_, err = db.ExecOrchestrator(`
			insert into access_token (
					public_token, secret_token, generated_at, generated_by, is_acquired, is_reentrant
				) values (
					?, ?, now(), ?, 0, 0
				)
			`,
		publicToken, secretToken, owner,
	)
	if err != nil {
		return publicToken, log.Errore(err)
	}
	return publicToken, nil
}

// AcquireAccessToken attempts to acquire a hopefully free token; returning in such case
// the secretToken as proof of ownership.
func AcquireAccessToken(publicToken string) (secretToken string, err error) {
	secretToken = ""
	sqlResult, err := db.ExecOrchestrator(`
			update access_token
				set
					is_acquired=1,
					acquired_at=now()
				where
					public_token=?
					and (
						(
							is_acquired=0
							and generated_at > now() - interval ? second
						)
						or is_reentrant=1
					)
			`,
		publicToken, config.Config.AccessTokenUseExpirySeconds,
	)
	if err != nil {
		return secretToken, log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return secretToken, log.Errore(err)
	}
	if rows == 0 {
		return secretToken, log.Errorf("Cannot acquire token %s", publicToken)
	}
	// Seems like we made it!
	query := `
		select secret_token from access_token where public_token=?
		`
	err = db.QueryOrchestrator(query, sqlutils.Args(publicToken), func(m sqlutils.RowMap) error {
		secretToken = m.GetString("secret_token")
		return nil
	})
	return secretToken, log.Errore(err)
}

// TokenIsValid checks to see whether a given token exists and is not outdated.
func TokenIsValid(publicToken string, secretToken string) (result bool, err error) {
	query := `
		select
				count(*) as valid_token
			from
				access_token
		  where
				public_token=?
				and secret_token=?
				and (
					generated_at >= now() - interval ? minute
					or is_reentrant = 1
				)
		`
	err = db.QueryOrchestrator(query, sqlutils.Args(publicToken, secretToken, config.Config.AccessTokenExpiryMinutes), func(m sqlutils.RowMap) error {
		result = m.GetInt("valid_token") > 0
		return nil
	})
	return result, log.Errore(err)
}

// ExpireAccessTokens removes old, known to be uneligible tokens
func ExpireAccessTokens() error {
	_, err := db.ExecOrchestrator(`
			delete
				from access_token
			where
				generated_at < now() - interval ? minute
				and is_reentrant = 0
			`,
		config.Config.AccessTokenExpiryMinutes,
	)
	return log.Errore(err)
}
