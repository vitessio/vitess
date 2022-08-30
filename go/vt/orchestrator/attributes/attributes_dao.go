/*
   Copyright 2014 Outbrain Inc.

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

package attributes

import (
	"fmt"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
)

// SetHostAttributes is used to set host attributes
func SetHostAttributes(hostname string, attributeName string, attributeValue string) error {
	_, err := db.ExecOrchestrator(`
			replace
				into host_attributes (
					hostname, attribute_name, attribute_value, submit_timestamp, expire_timestamp
				) VALUES (
					?, ?, ?, NOW(), NULL
				)
			`,
		hostname,
		attributeName,
		attributeValue,
	)
	if err != nil {
		log.Error(err)
		return err
	}

	return err
}

func getHostAttributesByClause(whereClause string, args []any) ([]HostAttributes, error) {
	var res []HostAttributes
	query := fmt.Sprintf(`
		select
			hostname,
			attribute_name,
			attribute_value,
			submit_timestamp ,
			ifnull(expire_timestamp, '') as expire_timestamp
		from
			host_attributes
		%s
		order by
			hostname, attribute_name
		`, whereClause)

	err := db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		hostAttributes := HostAttributes{}
		hostAttributes.Hostname = m.GetString("hostname")
		hostAttributes.AttributeName = m.GetString("attribute_name")
		hostAttributes.AttributeValue = m.GetString("attribute_value")
		hostAttributes.SubmitTimestamp = m.GetString("submit_timestamp")
		hostAttributes.ExpireTimestamp = m.GetString("expire_timestamp")

		res = append(res, hostAttributes)
		return nil
	})

	if err != nil {
		log.Error(err)
	}
	return res, err
}

// GetHostAttribute expects to return a single attribute for a given hostname/attribute-name combination
// or error on empty result
func GetHostAttribute(hostname string, attributeName string) (string, error) {
	whereClause := `where hostname=? and attribute_name=?`
	attributes, err := getHostAttributesByClause(whereClause, sqlutils.Args(hostname, attributeName))
	if err != nil {
		return "", err
	}
	if len(attributeName) == 0 {
		log.Errorf("No attribute found for %+v, %+v", hostname, attributeName)
		return "", fmt.Errorf("No attribute found for %+v, %+v", hostname, attributeName)
	}
	return attributes[0].AttributeValue, nil
}

// SetGeneralAttribute sets an attribute not associated with a host. Its a key-value thing
func SetGeneralAttribute(attributeName string, attributeValue string) error {
	if attributeName == "" {
		return nil
	}
	return SetHostAttributes("*", attributeName, attributeValue)
}

// GetGeneralAttribute expects to return a single attribute value (not associated with a specific hostname)
func GetGeneralAttribute(attributeName string) (result string, err error) {
	return GetHostAttribute("*", attributeName)
}
