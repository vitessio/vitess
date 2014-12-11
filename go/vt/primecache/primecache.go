// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// primecache primes the MySQL buffer cache with the rows that are
// going to be modified by the replication stream. It only activates
// if we're falling behind on replication.
package primecache

import (
	"bufio"
	"fmt"
	"io"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/golang/glog"

	"github.com/henryanand/vitess/go/mysql"
	"github.com/henryanand/vitess/go/vt/dbconfigs"
	vtenv "github.com/henryanand/vitess/go/vt/env"
)

// PrimeCache is the main object for this module: it handles a player
// that will run in a loop to prime the MySQL cache.
type PrimeCache struct {
	// set from constructor
	dbcfgs        *dbconfigs.DBConfigs
	relayLogsPath string

	// parameters with default values that can be changed by client
	WorkerCount   int
	SleepDuration time.Duration

	// reset for every run
	dbConn        *mysql.Connection
	workerChannel chan string
}

// NewPrimeCache creates a PrimeCache object with default parameters.
// The user can modify the public values before running the loop.
func NewPrimeCache(dbcfgs *dbconfigs.DBConfigs, relayLogsPath string) *PrimeCache {
	return &PrimeCache{
		dbcfgs:        dbcfgs,
		relayLogsPath: relayLogsPath,
		WorkerCount:   4,
		SleepDuration: 1 * time.Second,
	}
}

// slaveStatus is a small structure containing the info we need about
// replication
type slaveStatus struct {
	relayLogFile        string
	relayLogPos         string
	slaveSQLRunning     bool
	slaveIORunning      bool
	secondsBehindMaster uint64
	execMasterLogPos    uint64
}

// getSlaveStatus returns the known replication values.
func (pc *PrimeCache) getSlaveStatus() (*slaveStatus, error) {
	qr, err := pc.dbConn.ExecuteFetch("show slave status", 1, true)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 {
		return nil, fmt.Errorf("got unexpected row count, probably a master: %v", len(qr.Rows))
	}

	result := &slaveStatus{}
	for i, f := range qr.Fields {
		switch f.Name {
		case "Relay_Log_File":
			result.relayLogFile = qr.Rows[0][i].String()
		case "Relay_Log_Pos":
			result.relayLogPos = qr.Rows[0][i].String()
		case "Slave_SQL_Running":
			if qr.Rows[0][i].String() == "Yes" {
				result.slaveSQLRunning = true
			}
		case "Slave_IO_Running":
			if qr.Rows[0][i].String() == "Yes" {
				result.slaveIORunning = true
			}
		case "Seconds_Behind_Master":
			if !qr.Rows[0][i].IsNull() {
				var err error
				result.secondsBehindMaster, err = qr.Rows[0][i].ParseUint64()
				if err != nil {
					return nil, err
				}
			}
		case "Exec_Master_Log_Pos":
			if !qr.Rows[0][i].IsNull() {
				var err error
				result.execMasterLogPos, err = qr.Rows[0][i].ParseUint64()
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return result, nil
}

// applyLoop is the function run by the workers to empty the work queue.
func applyLoop(dbConn *mysql.Connection, c chan string) {
	for sql := range c {
		_, err := dbConn.ExecuteFetch(sql, 10000, false)
		if err != nil {
			// TODO(alainjobart) try to reconnect or just abort the whole loop
			// (feedback to main loop is tricky a bit)
			log.Warningf("Failed to execute sql '%v': %v", sql, err)
		}
	}

	dbConn.Close()
}

// setupPrimerConnections creates the channel and the consumers for
// the sql statements
func (pc *PrimeCache) setupPrimerConnections() error {
	pc.workerChannel = make(chan string, 1000)
	for i := 0; i < pc.WorkerCount; i++ {
		// connect to the database using client for a replay connection
		params, err := dbconfigs.MysqlParams(&pc.dbcfgs.App.ConnectionParams)
		if err != nil {
			return fmt.Errorf("cannot get parameters to connect to MySQL: %v", err)
		}

		dbConn, err := mysql.Connect(params)
		if err != nil {
			return fmt.Errorf("mysql.Connect failed: %v", err)
		}

		// and launch the go routine that applies the statements
		go applyLoop(dbConn, pc.workerChannel)
	}
	return nil
}

// openBinlog opens the binlog and returns a ReadCloser on them
func (pc *PrimeCache) openBinlog(slavestat *slaveStatus) (io.ReadCloser, error) {
	dir, err := vtenv.VtMysqlRoot()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(
		path.Join(dir, "bin/mysqlbinlog"),
		fmt.Sprintf("--start-position=%v", slavestat.relayLogPos),
		path.Join(pc.relayLogsPath, slavestat.relayLogFile),
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	err = cmd.Start()
	if err != nil {
		stdout.Close()
		return nil, err
	}
	return stdout, nil
}

// parseLogPos parses a comment line that has a end_log_pos in it.
// It returns isComment=true if the line is a comment.
// It may return pos!=0 if the line is a comment, and it has a end_log_pos.
func parseLogPos(line string) (pos uint64, isComment bool) {
	if !strings.HasPrefix(line, "#") {
		return
	}
	isComment = true

	elp := strings.Index(line, "end_log_pos ")
	if elp == -1 {
		return
	}

	line = line[elp+12:]
	end := strings.Index(line, " ")
	if end == -1 {
		return
	}

	line = line[:end]
	var err error
	pos, err = strconv.ParseUint(line[:end], 10, 64)
	if err != nil {
		log.Errorf("Failed to parse log_pos: %v", err)
		return
	}
	return
}

var deleteRegexp = regexp.MustCompile(`(?i)^delete\s+from\s+(\w+).*\s(where.*)`)

// parseDeleteStatement parses a delete statement.
// It returns isDelete=true if the line is a delete.
// It may return statement!="" if a statement can be run.
func parseDeleteStatement(line, lowerLine string) (statement string, isDelete bool) {
	if !strings.HasPrefix(lowerLine, "delete") {
		return
	}
	isDelete = true

	match := deleteRegexp.FindStringSubmatch(line)
	if match == nil {
		return
	}

	// match[0] has the full match
	// match[1] has the table
	// match[2] has the where clause
	statement = "select 1 ,'repl_primer' from " + match[1] + " " + match[2]
	return
}

var updateRegexp = regexp.MustCompile(`(?i)^update\s+(\w+).*\s(where.*)`)

// parseUpdateStatement parses an update statement.
// It returns isUpdate=true if the line is an update.
// It may return statement!="" if a statement can be run.
func parseUpdateStatement(line, lowerLine string) (statement string, isUpdate bool) {
	if !strings.HasPrefix(lowerLine, "update") {
		return
	}
	isUpdate = true

	match := updateRegexp.FindStringSubmatch(line)
	if match == nil {
		return
	}

	// match[0] has the full match
	// match[1] has the table
	// match[2] has the where clause
	statement = "select 1 ,'repl_primer' from " + match[1] + " " + match[2]
	return
}

// OneRun tries a single cycle connecting to MySQL, and if behind on
// replication, starts playing the logs ahead to prime the cache.
func (pc *PrimeCache) OneRun() {
	// connect to the database using dba for a control connection
	params, err := dbconfigs.MysqlParams(&pc.dbcfgs.Dba)
	if err != nil {
		log.Errorf("Cannot get parameters to connect to MySQL: %v", err)
		return
	}

	pc.dbConn, err = mysql.Connect(params)
	if err != nil {
		log.Errorf("mysql.Connect failed: %v", err)
		return
	}

	// get the slave status
	slavestat, err := pc.getSlaveStatus()
	if err != nil {
		log.Warningf("getSlaveStatus failed: %v", err)
		return
	}

	// if we're not replicating, we're done
	if !slavestat.slaveSQLRunning {
		log.Warningf("Slave is not replicating (SQL)")
		return
	}
	if !slavestat.slaveIORunning {
		log.Warningf("Slave is not replicating (IO)")
		return
	}
	if slavestat.secondsBehindMaster < 2 {
		return
	}
	log.Infof("Replication lag is high (%v seconds), activating", slavestat.secondsBehindMaster)

	// setup the connections to the db to apply the statements
	if err := pc.setupPrimerConnections(); err != nil {
		log.Errorf("setupPrimerConnections failed: %v", err)
		return
	}

	// open the binlogs from where we are on
	reader, err := pc.openBinlog(slavestat)
	if err != nil {
		log.Errorf("openBinlog failed: %v", err)
		return
	}

	maxLineCount := 10000
	var maxLeadBytes int64 = 5000000

	// and start the loop
	lineCount := 0
	deleteCount := 0
	appliedDeleteCount := 0
	updateCount := 0
	appliedUpdateCount := 0
	sleepCount := 0
	var logPos uint64
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		lowerLine := strings.ToLower(line)
		lineCount++

		if p, isComment := parseLogPos(line); isComment {
			// handle the comments with a log pos
			if p > logPos {
				logPos = p
			}

		} else if s, isDelete := parseDeleteStatement(line, lowerLine); isDelete {
			// handle delete statements
			deleteCount++
			if s != "" {
				appliedDeleteCount++
				pc.workerChannel <- s
			}

		} else if s, isUpdate := parseUpdateStatement(line, lowerLine); isUpdate {
			// handle update statements
			updateCount++
			if s != "" {
				appliedUpdateCount++
				pc.workerChannel <- s
			}
		}

		if lineCount%maxLineCount == 0 {
			var leadBytes int64
			for {
				slavestat, err = pc.getSlaveStatus()
				if err != nil {
					log.Errorf("getSlaveStatus failed: %v", err)
					return
				}

				// see how far ahead we are (it's signed because
				// we can be behind too)
				leadBytes = int64(logPos) - int64(slavestat.execMasterLogPos)
				if leadBytes > maxLeadBytes {
					sleepCount++
					log.Infof("Sleeping for 1 second waiting for SQL thread to advance: %v > %v", leadBytes, maxLeadBytes)
					time.Sleep(1 * time.Second)
					continue
				} else {
					break
				}
			}

			log.Infof("STATS: readahead: %10d lag: %7d sleeps: %4d deletes: %10d missed updates: %10d updates: %10d\n",
				leadBytes, slavestat.secondsBehindMaster, sleepCount, deleteCount, updateCount-appliedUpdateCount, appliedUpdateCount)
		}
	}
	reader.Close()
	if err := scanner.Err(); err != nil {
		log.Errorf("Scanner failed: %v", err)
	}
}

// Cleanup will release all resources help by this object.
// Closing pc.workerChannel will end up finishing the consumer tasks.
func (pc *PrimeCache) Cleanup() {
	if pc.dbConn != nil {
		pc.dbConn.Close()
		pc.dbConn = nil
	}
	if pc.workerChannel != nil {
		close(pc.workerChannel)
		pc.workerChannel = nil
	}
}

// Loop runs the primecache loop and never returns.
func (pc *PrimeCache) Loop() {
	for {
		log.Info("Prime cache starting run")
		pc.OneRun()
		pc.Cleanup()
		time.Sleep(pc.SleepDuration)
	}
}
