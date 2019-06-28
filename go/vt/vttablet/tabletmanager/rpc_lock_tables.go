/*
Copyright 2017 Google Inc.

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

package tabletmanager

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"

	"golang.org/x/net/context"
)

var (
	lockTablesTimeout = flag.Duration("lock_tables_timeout", 1*time.Minute, "How long to keep the table locked before timing out")
)

// LockTables will lock all tables with read locks, effectively pausing replication while the lock is held (idempotent)
func (agent *ActionAgent) LockTables(ctx context.Context) error {
	// get a connection
	agent.mutex.Lock()
	defer agent.mutex.Unlock()

	if agent._lockTablesConnection != nil {
		// tables are already locked, bail out
		return errors.New("tables already locked on this tablet")
	}

	conn, err := agent.MysqlDaemon.GetDbaConnection()
	if err != nil {
		return err
	}

	// FTWRL is preferable, so we'll try that first
	_, err = conn.ExecuteFetch("FLUSH TABLES WITH READ LOCK", 0, false)
	if err != nil {
		// as fall back, we can lock each individual table as well.
		// this requires slightly less privileges but achieves the same effect
		err = agent.lockTablesUsingLockTables(conn)
		if err != nil {
			return err
		}
	}
	log.Infof("[%v] Tables locked", conn.ConnectionID)

	agent._lockTablesConnection = conn
	agent._lockTablesTimer = time.AfterFunc(*lockTablesTimeout, func() {
		// Here we'll sleep until the timeout time has elapsed.
		// If the table locks have not been released yet, we'll release them here
		agent.mutex.Lock()
		defer agent.mutex.Unlock()

		// We need the mutex locked before we check this field
		if agent._lockTablesConnection == conn {
			log.Errorf("table lock timed out and released the lock - something went wrong")
			err = agent.unlockTablesHoldingMutex()
			if err != nil {
				log.Errorf("failed to unlock tables: %v", err)
			}
		}
	})

	return nil
}

func (agent *ActionAgent) lockTablesUsingLockTables(conn *dbconnpool.DBConnection) error {
	log.Warningf("failed to lock tables with FTWRL - falling back to LOCK TABLES")

	// Ensure schema engine is Open. If vttablet came up in a non_serving role,
	// the schema engine may not have been initialized. Open() is idempotent, so this
	// is always safe
	se := agent.QueryServiceControl.SchemaEngine()
	if err := se.Open(); err != nil {
		return err
	}

	tables := se.GetSchema()
	tableNames := make([]string, 0, len(tables))
	for name := range tables {
		if name == "dual" {
			continue
		}
		tableNames = append(tableNames, fmt.Sprintf("%s READ", sqlescape.EscapeID(name)))
	}
	lockStatement := fmt.Sprintf("LOCK TABLES %v", strings.Join(tableNames, ", "))
	_, err := conn.ExecuteFetch(fmt.Sprintf("USE %s", agent.DBConfigs.DBName.Get()), 0, false)
	if err != nil {
		return err
	}

	_, err = conn.ExecuteFetch(lockStatement, 0, false)
	if err != nil {
		return err
	}

	return nil
}

// UnlockTables will unlock all tables (idempotent)
func (agent *ActionAgent) UnlockTables(ctx context.Context) error {
	agent.mutex.Lock()
	defer agent.mutex.Unlock()

	if agent._lockTablesConnection == nil {
		return fmt.Errorf("tables were not locked")
	}

	return agent.unlockTablesHoldingMutex()
}

func (agent *ActionAgent) unlockTablesHoldingMutex() error {
	// We are cleaning up manually, let's kill the timer
	agent._lockTablesTimer.Stop()
	_, err := agent._lockTablesConnection.ExecuteFetch("UNLOCK TABLES", 0, false)
	if err != nil {
		return err
	}
	log.Infof("[%v] Tables unlocked", agent._lockTablesConnection.ConnectionID)
	agent._lockTablesConnection.Close()
	agent._lockTablesConnection = nil
	agent._lockTablesTimer = nil

	return nil
}
