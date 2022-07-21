/*
Copyright 2019 The Vitess Authors.

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

	"context"
)

var (
	lockTablesTimeout = flag.Duration("lock_tables_timeout", 1*time.Minute, "How long to keep the table locked before timing out")
)

// LockTables will lock all tables with read locks, effectively pausing replication while the lock is held (idempotent)
func (tm *TabletManager) LockTables(ctx context.Context) error {
	// get a connection
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if tm._lockTablesConnection != nil {
		// tables are already locked, bail out
		return errors.New("tables already locked on this tablet")
	}

	conn, err := tm.MysqlDaemon.GetDbaConnection(ctx)
	if err != nil {
		return err
	}
	// We successfully opened a connection. If we return for any reason before
	// storing this connection in the TabletManager object, we need to close it
	// to avoid leaking it.
	defer func() {
		if tm._lockTablesConnection != conn {
			conn.Close()
		}
	}()

	// FTWRL is preferable, so we'll try that first
	_, err = conn.ExecuteFetch("FLUSH TABLES WITH READ LOCK", 0, false)
	if err != nil {
		// as fall back, we can lock each individual table as well.
		// this requires slightly less privileges but achieves the same effect
		err = tm.lockTablesUsingLockTables(conn)
		if err != nil {
			return err
		}
	}
	log.Infof("[%v] Tables locked", conn.ConnectionID)

	tm._lockTablesConnection = conn
	tm._lockTablesTimer = time.AfterFunc(*lockTablesTimeout, func() {
		// Here we'll sleep until the timeout time has elapsed.
		// If the table locks have not been released yet, we'll release them here
		tm.mutex.Lock()
		defer tm.mutex.Unlock()

		// We need the mutex locked before we check this field
		if tm._lockTablesConnection == conn {
			log.Errorf("table lock timed out and released the lock - something went wrong")
			err = tm.unlockTablesHoldingMutex()
			if err != nil {
				log.Errorf("failed to unlock tables: %v", err)
			}
		}
	})

	return nil
}

func (tm *TabletManager) lockTablesUsingLockTables(conn *dbconnpool.DBConnection) error {
	log.Warningf("failed to lock tables with FTWRL - falling back to LOCK TABLES")

	// Ensure schema engine is Open. If vttablet came up in a non_serving role,
	// the schema engine may not have been initialized. Open() is idempotent, so this
	// is always safe
	se := tm.QueryServiceControl.SchemaEngine()
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
	_, err := conn.ExecuteFetch("USE "+sqlescape.EscapeID(tm.DBConfigs.DBName), 0, false)
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
func (tm *TabletManager) UnlockTables(ctx context.Context) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if tm._lockTablesConnection == nil {
		return fmt.Errorf("tables were not locked")
	}

	return tm.unlockTablesHoldingMutex()
}

func (tm *TabletManager) unlockTablesHoldingMutex() error {
	// We are cleaning up manually, let's kill the timer
	tm._lockTablesTimer.Stop()
	_, err := tm._lockTablesConnection.ExecuteFetch("UNLOCK TABLES", 0, false)
	if err != nil {
		return err
	}
	log.Infof("[%v] Tables unlocked", tm._lockTablesConnection.ConnectionID)
	tm._lockTablesConnection.Close()
	tm._lockTablesConnection = nil
	tm._lockTablesTimer = nil

	return nil
}
