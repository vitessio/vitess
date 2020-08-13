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
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
)

func WriteMasterPositionEquivalence(master1Key *InstanceKey, master1BinlogCoordinates *BinlogCoordinates,
	master2Key *InstanceKey, master2BinlogCoordinates *BinlogCoordinates) error {
	if master1Key.Equals(master2Key) {
		// Not interesting
		return nil
	}
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
        	insert into master_position_equivalence (
        			master1_hostname, master1_port, master1_binary_log_file, master1_binary_log_pos,
        			master2_hostname, master2_port, master2_binary_log_file, master2_binary_log_pos,
        			last_suggested)
        		values (?, ?, ?, ?, ?, ?, ?, ?, NOW()) 
        		on duplicate key update last_suggested=values(last_suggested)
				
				`, master1Key.Hostname, master1Key.Port, master1BinlogCoordinates.LogFile, master1BinlogCoordinates.LogPos,
			master2Key.Hostname, master2Key.Port, master2BinlogCoordinates.LogFile, master2BinlogCoordinates.LogPos,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

func GetEquivalentMasterCoordinates(instanceCoordinates *InstanceBinlogCoordinates) (result [](*InstanceBinlogCoordinates), err error) {
	query := `
		select 
				master1_hostname as hostname,
				master1_port as port,
				master1_binary_log_file as binlog_file,
				master1_binary_log_pos as binlog_pos
			from 
				master_position_equivalence
			where
				master2_hostname = ?
				and master2_port = ?
				and master2_binary_log_file = ?
				and master2_binary_log_pos = ?
		union
		select 
				master2_hostname as hostname,
				master2_port as port,
				master2_binary_log_file as binlog_file,
				master2_binary_log_pos as binlog_pos
			from 
				master_position_equivalence
			where
				master1_hostname = ?
				and master1_port = ?
				and master1_binary_log_file = ?
				and master1_binary_log_pos = ?
		`
	args := sqlutils.Args(
		instanceCoordinates.Key.Hostname,
		instanceCoordinates.Key.Port,
		instanceCoordinates.Coordinates.LogFile,
		instanceCoordinates.Coordinates.LogPos,
		instanceCoordinates.Key.Hostname,
		instanceCoordinates.Key.Port,
		instanceCoordinates.Coordinates.LogFile,
		instanceCoordinates.Coordinates.LogPos,
	)

	err = db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		equivalentCoordinates := InstanceBinlogCoordinates{}
		equivalentCoordinates.Key.Hostname = m.GetString("hostname")
		equivalentCoordinates.Key.Port = m.GetInt("port")
		equivalentCoordinates.Coordinates.LogFile = m.GetString("binlog_file")
		equivalentCoordinates.Coordinates.LogPos = m.GetInt64("binlog_pos")

		result = append(result, &equivalentCoordinates)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func GetEquivalentBinlogCoordinatesFor(instanceCoordinates *InstanceBinlogCoordinates, belowKey *InstanceKey) (*BinlogCoordinates, error) {
	possibleCoordinates, err := GetEquivalentMasterCoordinates(instanceCoordinates)
	if err != nil {
		return nil, err
	}
	for _, instanceCoordinates := range possibleCoordinates {
		if instanceCoordinates.Key.Equals(belowKey) {
			return &instanceCoordinates.Coordinates, nil
		}
	}
	return nil, nil
}

// ExpireMasterPositionEquivalence expires old master_position_equivalence
func ExpireMasterPositionEquivalence() error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
        	delete from master_position_equivalence 
				where last_suggested < NOW() - INTERVAL ? HOUR
				`, config.Config.UnseenInstanceForgetHours,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}
