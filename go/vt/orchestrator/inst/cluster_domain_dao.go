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

package inst

import (
	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
)

// WriteClusterDomainName will write (and override) the domain name of a cluster
func WriteClusterDomainName(clusterName string, domainName string) error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
			insert into
					cluster_domain_name (cluster_name, domain_name, last_registered)
				values
					(?, ?, NOW())
				on duplicate key update
					domain_name=values(domain_name),
					last_registered=values(last_registered)
			`,
			clusterName, domainName)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

// ExpireClusterDomainName expires cluster_domain_name entries that haven't been updated recently.
func ExpireClusterDomainName() error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
    	delete from cluster_domain_name
				where last_registered < NOW() - INTERVAL ? MINUTE
				`, config.Config.ExpiryHostnameResolvesMinutes,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}
