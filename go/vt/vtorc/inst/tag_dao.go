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
	"fmt"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtorc/db"
)

func PutInstanceTag(instanceKey *InstanceKey, tag *Tag) (err error) {
	_, err = db.ExecVTOrc(`
			insert
				into database_instance_tags (
					hostname, port, tag_name, tag_value, last_updated
				) VALUES (
					?, ?, ?, ?, NOW()
				)
				on duplicate key update
					tag_value=values(tag_value),
					last_updated=values(last_updated)
			`,
		instanceKey.Hostname,
		instanceKey.Port,
		tag.TagName,
		tag.TagValue,
	)
	return err
}

func Untag(instanceKey *InstanceKey, tag *Tag) (tagged *InstanceKeyMap, err error) {
	if tag == nil {
		errMsg := "untag: tag is nil"
		log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	if tag.Negate {
		errMsg := "untag: does not support negation"
		log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	if instanceKey == nil && !tag.HasValue {
		errMsg := "untag: either indicate an instance or a tag value. Will not delete on-valued tag across instances"
		log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	var clause string
	args := sqlutils.Args()
	if tag.HasValue {
		clause = `tag_name=? and tag_value=?`
		args = append(args, tag.TagName, tag.TagValue)
	} else {
		clause = `tag_name=?`
		args = append(args, tag.TagName)
	}
	if instanceKey != nil {
		clause = fmt.Sprintf("%s and hostname=? and port=?", clause)
		args = append(args, instanceKey.Hostname, instanceKey.Port)
	}
	tagged = NewInstanceKeyMap()
	query := fmt.Sprintf(`
		select
			hostname,
			port
		from
			database_instance_tags
		where
			%s
		order by hostname, port
		`, clause,
	)
	_ = db.QueryVTOrc(query, args, func(m sqlutils.RowMap) error {
		key, _ := NewResolveInstanceKey(m.GetString("hostname"), m.GetInt("port"))
		tagged.AddKey(*key)
		return nil
	})

	query = fmt.Sprintf(`
			delete from
				database_instance_tags
			where
				%s
			`, clause,
	)
	if _, err = db.ExecVTOrc(query, args...); err != nil {
		log.Error(err)
		return tagged, err
	}
	_ = AuditOperation("delete-instance-tag", instanceKey, tag.String())
	return tagged, nil
}

func ReadInstanceTag(instanceKey *InstanceKey, tag *Tag) (tagExists bool, err error) {
	query := `
		select
			tag_value
		from
			database_instance_tags
		where
			hostname = ?
			and port = ?
			and tag_name = ?
			`
	args := sqlutils.Args(instanceKey.Hostname, instanceKey.Port, tag.TagName)
	err = db.QueryVTOrc(query, args, func(m sqlutils.RowMap) error {
		tag.TagValue = m.GetString("tag_value")
		tagExists = true
		return nil
	})

	if err != nil {
		log.Error(err)
	}
	return tagExists, err
}

func ReadInstanceTags(instanceKey *InstanceKey) (tags [](*Tag), err error) {
	tags = [](*Tag){}
	query := `
		select
			tag_name, tag_value
		from
			database_instance_tags
		where
			hostname = ?
			and port = ?
		order by tag_name
			`
	args := sqlutils.Args(instanceKey.Hostname, instanceKey.Port)
	err = db.QueryVTOrc(query, args, func(m sqlutils.RowMap) error {
		tag := &Tag{
			TagName:  m.GetString("tag_name"),
			TagValue: m.GetString("tag_value"),
		}
		tags = append(tags, tag)
		return nil
	})

	if err != nil {
		log.Error(err)
	}
	return tags, err
}

func GetInstanceKeysByTag(tag *Tag) (tagged *InstanceKeyMap, err error) {
	if tag == nil {
		errMsg := "GetInstanceKeysByTag: tag is nil"
		log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	clause := ``
	args := sqlutils.Args()
	if tag.HasValue && !tag.Negate {
		// exists and equals
		clause = `tag_name=? and tag_value=?`
		args = append(args, tag.TagName, tag.TagValue)
	} else if !tag.HasValue && !tag.Negate {
		// exists
		clause = `tag_name=?`
		args = append(args, tag.TagName)
	} else if tag.HasValue && tag.Negate {
		// exists and not equal
		clause = `tag_name=? and tag_value!=?`
		args = append(args, tag.TagName, tag.TagValue)
	} else if !tag.HasValue && tag.Negate {
		// does not exist
		clause = `1=1 group by hostname, port having sum(tag_name=?)=0`
		args = append(args, tag.TagName)
	}
	tagged = NewInstanceKeyMap()
	query := fmt.Sprintf(`
		select
			hostname,
			port
		from
			database_instance_tags
		where
			%s
		order by hostname, port
		`, clause)
	err = db.QueryVTOrc(query, args, func(m sqlutils.RowMap) error {
		key, _ := NewResolveInstanceKey(m.GetString("hostname"), m.GetInt("port"))
		tagged.AddKey(*key)
		return nil
	})
	if err != nil {
		log.Error(err)
	}
	return tagged, err
}
