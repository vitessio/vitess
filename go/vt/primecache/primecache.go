// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// primecache primes the mysql buffer cache with the rows that are
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

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	vtenv "github.com/youtube/vitess/go/vt/env"
	"github.com/youtube/vitess/go/vt/mysqlctl"
)

type PrimeCache struct {
	// set from constructor
	dbcfgs      *dbconfigs.DBConfigs
	mycnf       *mysqlctl.Mycnf
	workerCount int

	// reset for every run
	dbConn         *mysql.Connection
	workerChannels []chan string
}

func NewPrimeCache(dbcfgs *dbconfigs.DBConfigs, mycnf *mysqlctl.Mycnf) *PrimeCache {
	return &PrimeCache{
		dbcfgs:      dbcfgs,
		mycnf:       mycnf,
		workerCount: 4,
	}
}

type slaveStatus struct {
	relayLogFile        string
	relayLogPos         string
	slaveSQLRunning     bool
	slaveIORunning      bool
	secondsBehindMaster uint64
	execMasterLogPos    uint64
}

func (pc *PrimeCache) GetSlaveStatus() (*slaveStatus, error) {
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

func ApplyLoop(dbConn *mysql.Connection, c chan string) {
	for sql := range c {
		_, err := dbConn.ExecuteFetch(sql, 10000, false)
		if err != nil {
			// TODO(alainjobart) try to reconnect
			log.Warningf("Failed to execute sql '%v': %v", sql, err)
		}
	}

	dbConn.Close()
}

func (pc *PrimeCache) SetupPrimerConnections() error {
	pc.workerChannels = make([]chan string, pc.workerCount)
	for i := 0; i < pc.workerCount; i++ {
		pc.workerChannels[i] = make(chan string, 100)

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
		go ApplyLoop(dbConn, pc.workerChannels[i])
	}
	return nil
}

func (pc *PrimeCache) Cleanup() {
	if pc.dbConn != nil {
		pc.dbConn.Close()
		pc.dbConn = nil
	}
	if pc.workerChannels != nil {
		for _, c := range pc.workerChannels {
			close(c)
		}
		pc.workerChannels = nil
	}
}

func (pc *PrimeCache) OpenBinlog(slavestat *slaveStatus) (io.ReadCloser, error) {
	dir, err := vtenv.VtMysqlRoot()
	if err != nil {
		return nil, err
	}
	log.Infof("Running %v/bin/mysqlbinlog --start-position=%v %v/%v", dir, slavestat.relayLogPos, pc.mycnf.RelayLogPath, slavestat.relayLogFile)
	cmd := exec.Command(
		path.Join(dir, "bin/mysqlbinlog"),
		fmt.Sprintf("--start-position=%v", slavestat.relayLogPos),
		path.Join(pc.mycnf.RelayLogPath, slavestat.relayLogFile),
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

// parseLogPos parse a comment line that has a log_pos() in it.
// It returns isComment=true if the line is a comment.
// It may return pos!=0 if the line is a comment, and it has a log_pos.
func parseLogPos(line string) (pos uint64, isComment bool) {
	if !strings.HasPrefix(line, "#") {
		return
	}
	isComment = true

	lpi := strings.Index(line, "log_pos (")
	if lpi == -1 {
		return
	}

	line = line[lpi+9:]
	end := strings.Index(line, ")")
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
	slavestat, err := pc.GetSlaveStatus()
	if err != nil {
		log.Errorf("GetSlaveStatus failed: %v", err)
		return
	}

	// if we're not replicating, we're done
	if !slavestat.slaveSQLRunning {
		log.Errorf("Slave is not replicating (SQL)")
		return
	}
	if !slavestat.slaveIORunning {
		log.Errorf("Slave is not replicating (IO)")
		return
	}
	if slavestat.secondsBehindMaster < 2 {
		log.Errorf("Slave lag is negligible - %v seconds", slavestat.secondsBehindMaster)
		return
	}

	// setup the connections to the db to apply the statements
	if err := pc.SetupPrimerConnections(); err != nil {
		log.Errorf("SetupPrimerConnections failed: %v", err)
		return
	}

	// open the binlogs from where we are on
	reader, err := pc.OpenBinlog(slavestat)
	if err != nil {
		log.Errorf("OpenBinlog failed: %v", err)
		return
	}

	maxLineCount := 10000
	var maxLeadBytes uint64 = 5000000

	// and start the loop
	lineCount := 0
	deleteCount := 0
	appliedDeleteCount := 0
	updateCount := 0
	appliedUpdateCount := 0
	sleepCount := 0
	var logPos uint64
	workerIndex := 0
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		lineCount++

		println(line)

		// handle the comments with a log pos
		if p, isComment := parseLogPos(line); isComment {
			if p > logPos {
				logPos = p
			}
			continue
		}

		// handle deletes
		lowerLine := strings.ToLower(line)
		if s, isDelete := parseDeleteStatement(line, lowerLine); isDelete {
			deleteCount++
			if s != "" {
				appliedDeleteCount++
				pc.workerChannels[workerIndex%pc.workerCount] <- s
				workerIndex++
			}
			continue
		}

		// handle updates
		if s, isUpdate := parseUpdateStatement(line, lowerLine); isUpdate {
			updateCount++
			if s != "" {
				appliedUpdateCount++
				pc.workerChannels[workerIndex%pc.workerCount] <- s
				workerIndex++
			}
			continue
		}

		if lineCount%maxLineCount == 0 {
			var leadBytes uint64
			for {
				slavestat, err = pc.GetSlaveStatus()
				if err != nil {
					log.Errorf("GetSlaveStatus failed: %v", err)
					return
				}

				leadBytes = logPos - slavestat.execMasterLogPos
				if leadBytes > maxLeadBytes {
					sleepCount++
					log.Infof("Sleeping for 1 second waiting for SQL thread to advance")
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
	if err := scanner.Err(); err != nil {
		log.Errorf("Scanner failed: %v", err)
	}
}

func (pc *PrimeCache) Loop() {
	for {
		log.Info("Prime cache starting run")
		pc.OneRun()
		pc.Cleanup()
		time.Sleep(1 * time.Second)
	}
}
