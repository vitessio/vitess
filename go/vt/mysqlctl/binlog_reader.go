// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

/*
the binlogreader is intended to "tail -f" a binlog, but be smart enough
to stop tailing it when mysql is done writing to that binlog.  The stop
condition is if EOF is reached *and* the next file has appeared.
*/

import (
	"code.google.com/p/vitess/go/relog"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	BINLOG_HEADER_SIZE = 4  // copied from mysqlbinlog.cc for mysql 5.0.33
	EVENT_HEADER_SIZE  = 19 // 4.0 and above, can be larger in 5.x
	BINLOG_BLOCK_SIZE  = 16 * 1024
	MYSQLBINLOG_CHUNK  = 64 * 1024
	MAX_WAIT_TIMEOUT   = 30.0
	LOG_WAIT_TIMEOUT   = 5.0
)

type stats struct {
	Reads     int64
	Bytes     int64
	Sleeps    int64
	StartTime time.Time
}

type request struct {
	config        *Mycnf
	startPosition int64
	file          *os.File
	nextFilename  string
	stats
}

type BinlogReader struct {
	binLogPrefix string

	// these parameters will have reasonable default values but can be tuned
	BinlogBlockSize int64
	MaxWaitTimeout  float64
	LogWaitTimeout  float64
}

func (blr *BinlogReader) binLogPathForId(fileId int) string {
	return fmt.Sprintf("%v.%06d", blr.binLogPrefix, fileId)
}

func NewBinlogReader(binLogPrefix string) *BinlogReader {
	return &BinlogReader{binLogPrefix: binLogPrefix,
		BinlogBlockSize: BINLOG_BLOCK_SIZE,
		MaxWaitTimeout:  MAX_WAIT_TIMEOUT,
		LogWaitTimeout:  LOG_WAIT_TIMEOUT}
}

/*
 based on http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
 +=====================================+
 | event  | timestamp         0 : 4    |
 | header +----------------------------+
 |        | type_code         4 : 1    |
 |        +----------------------------+
 |        | server_id         5 : 4    |
 |        +----------------------------+
 |        | event_length      9 : 4    |
 |        +----------------------------+
 |        | next_position    13 : 4    |
 |        +----------------------------+
 |        | flags            17 : 2    |
 +=====================================+
 | event  | fixed part       19 : y    |
 | data   +----------------------------+
 |        | variable part              |
 +=====================================+
*/
func readFirstEventSize(binlog io.ReadSeeker) uint32 {
	pos, _ := binlog.Seek(0, 1)
	defer binlog.Seek(pos, 0)

	if _, err := binlog.Seek(BINLOG_HEADER_SIZE+9, 0); err != nil {
		panic("failed binlog seek: " + err.Error())
	}

	var eventLength uint32
	if err := binary.Read(binlog, binary.LittleEndian, &eventLength); err != nil {
		panic("failed binlog read: " + err.Error())
	}
	return eventLength
}

func (blr *BinlogReader) serve(filename string, startPosition int64, writer http.ResponseWriter) {
	flusher := writer.(http.Flusher)
	stats := stats{StartTime: time.Now()}

	binlogFile, nextLog := blr.open(filename)
	defer binlogFile.Close()
	positionWaitStart := make(map[int64]time.Time)

	if startPosition > 0 {
		// the start position can be greater than the file length
		// in which case, we just keep rotating files until we find it
		for {
			size, err := binlogFile.Seek(0, 2)
			if err != nil {
				relog.Error("BinlogReader.serve seek err: %v", err)
				return
			}
			if startPosition > size {
				startPosition -= size

				// swap to next file
				binlogFile.Close()
				binlogFile, nextLog = blr.open(nextLog)

				// normally we chomp subsequent headers, so we have to
				// add this back into the position
				//startPosition += BINLOG_HEADER_SIZE
			} else {
				break
			}
		}

		// inject the header again to fool mysqlbinlog
		// FIXME(msolomon) experimentally determine the header size.
		// 5.1.50 is 106, 5.0.24 is 98
		firstEventSize := readFirstEventSize(binlogFile)
		prefixSize := int64(BINLOG_HEADER_SIZE + firstEventSize)
		writer.Header().Set("Vt-Binlog-Offset", strconv.FormatInt(prefixSize, 10))
		relog.Info("BinlogReader.serve inject header + first event: %v", prefixSize)

		position, err := binlogFile.Seek(0, 0)
		if err == nil {
			_, err = io.CopyN(writer, binlogFile, prefixSize)
			//relog.Info("BinlogReader %x copy @ %v:%v,%v", stats.StartTime, binlogFile.Name(), position, written)
		}
		if err != nil {
			relog.Error("BinlogReader.serve err: %v", err)
			return
		}
		position, err = binlogFile.Seek(startPosition, 0)
		relog.Info("BinlogReader %x seek to startPosition %v @ %v:%v", stats.StartTime, startPosition, binlogFile.Name(), position)
	} else {
		writer.Header().Set("Vt-Binlog-Offset", "0")
	}

	// FIXME(msolomon) register stats on http handler
	for {
		//position, _ := binlogFile.Seek(0, 1)
		written, err := io.CopyN(writer, binlogFile, blr.BinlogBlockSize)
		//relog.Info("BinlogReader %x copy @ %v:%v,%v", stats.StartTime, binlogFile.Name(), position, written)
		if err != nil && err != io.EOF {
			relog.Error("BinlogReader.serve err: %v", err)
			return
		}

		stats.Reads++
		stats.Bytes += written

		if written != blr.BinlogBlockSize {
			if _, statErr := os.Stat(nextLog); statErr == nil {
				relog.Info("BinlogReader swap log file: %v", nextLog)
				// swap to next log file
				binlogFile.Close()
				binlogFile, nextLog = blr.open(nextLog)
				positionWaitStart = make(map[int64]time.Time)
				binlogFile.Seek(BINLOG_HEADER_SIZE, 0)
			} else {
				flusher.Flush()
				position, _ := binlogFile.Seek(0, 1)
				relog.Info("BinlogReader %x wait for more data: %v:%v", stats.StartTime, binlogFile.Name(), position)
				// wait for more data
				time.Sleep(time.Duration(blr.LogWaitTimeout * 1e9))
				stats.Sleeps++
				now := time.Now()
				if lastSlept, ok := positionWaitStart[position]; ok {
					if (now.Sub(lastSlept)) > time.Duration(blr.MaxWaitTimeout*1e9) {
						relog.Error("MAX_WAIT_TIMEOUT exceeded, closing connection")
						return
					}
				} else {
					positionWaitStart[position] = now
				}
			}
		}
	}
}

func (blr *BinlogReader) HandleBinlogRequest(rw http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			// nothing to do, but note it here and soldier on
			relog.Error("HandleBinlogRequest failed: %v", err)
		}
	}()

	// FIXME(msolomon) some sort of security, no?
	relog.Info("serve %v", req.URL.Path)
	// path is something like /vt/vt-xxxxxx-bin-log:position
	pieces := strings.SplitN(path.Base(req.URL.Path), ":", 2)
	pos, _ := strconv.ParseInt(pieces[1], 10, 64)
	blr.serve(pieces[0], pos, rw)
}

// return open log file and the name of the next log path to watch
func (blr *BinlogReader) open(name string) (*os.File, string) {
	ext := path.Ext(name)
	fileId, err := strconv.Atoi(ext[1:])
	if err != nil {
		panic(fmt.Errorf("bad binlog name: %v", name))
	}
	logPath := blr.binLogPathForId(fileId)
	if !strings.HasSuffix(logPath, name) {
		panic(fmt.Errorf("binlog name mismatch: %v vs %v", logPath, name))
	}
	file, err := os.Open(logPath)
	if err != nil {
		panic(err)
	}
	nextLog := blr.binLogPathForId(fileId + 1)
	return file, nextLog
}

func (blr *BinlogReader) ServeData(writer io.Writer, filename string, startPosition int64) {
	stats := stats{StartTime: time.Now()}

	binlogFile, nextLog := blr.open(filename)
	defer binlogFile.Close()
	positionWaitStart := make(map[int64]time.Time)

	if startPosition > 0 {
		size, err := binlogFile.Seek(0, 2)
		if err != nil {
			panic(fmt.Errorf("BinlogReader.ServeData seek err: %v", err))
		}
		if startPosition > size {
			panic(fmt.Errorf("BinlogReader.ServeData: start position %v greater than size %v", startPosition, size))
		}

		// inject the header again to fool mysqlbinlog
		// FIXME(msolomon) experimentally determine the header size.
		// 5.1.50 is 106, 5.0.24 is 98
		firstEventSize := readFirstEventSize(binlogFile)
		prefixSize := int64(BINLOG_HEADER_SIZE + firstEventSize)

		position, err := binlogFile.Seek(0, 0)
		if err == nil {
			_, err = io.CopyN(writer, binlogFile, prefixSize)
		}
		if err != nil {
			panic(fmt.Errorf("BinlogReader.ServeData err: %v", err))
		}
		position, err = binlogFile.Seek(startPosition, 0)
		if err != nil {
			panic(fmt.Errorf("Failed BinlogReader seek to startPosition %v @ %v:%v", startPosition, binlogFile.Name(), position))
		}
	}

	buf := make([]byte, blr.BinlogBlockSize)
	for {
		buf = buf[:0]
		position, _ := binlogFile.Seek(0, 1)
		written, err := copyBufN(writer, binlogFile, blr.BinlogBlockSize, buf)
		if err != nil && err != io.EOF {
			panic(fmt.Errorf("BinlogReader.serve err: %v", err))
		}
		//relog.Info("BinlogReader copy @ %v:%v,%v", binlogFile.Name(), position, written)

		stats.Reads++
		stats.Bytes += written

		//EOF is implicit in this case - either we have reached end of current file
		//and are waiting for more data or rotating to next log.
		if written != blr.BinlogBlockSize {
			if _, statErr := os.Stat(nextLog); statErr == nil {
				// swap to next log file
				size, _ := binlogFile.Seek(0, 2)
				if size == position+written {
					//relog.Info("BinlogReader swap log file: %v", nextLog)
					binlogFile.Close()
					binlogFile, nextLog = blr.open(nextLog)
					positionWaitStart = make(map[int64]time.Time)
					binlogFile.Seek(BINLOG_HEADER_SIZE, 0)
				}
			} else {
				position, _ := binlogFile.Seek(0, 1)
				//relog.Info("BinlogReader wait for more data: %v:%v %v", binlogFile.Name(), position, time.Duration(blr.LogWaitTimeout * 1e9))
				// wait for more data
				time.Sleep(time.Duration(blr.LogWaitTimeout * 1e9))
				stats.Sleeps++
				now := time.Now()
				if lastSlept, ok := positionWaitStart[position]; ok {
					if (now.Sub(lastSlept)) > time.Duration(blr.MaxWaitTimeout*1e9) {
						//vt_mysqlbinlog reads in chunks of 64k bytes, the code below pads null bytes so the remaining data
						//in the buffer can be flushed before closing this stream. This manifests itself as end of log file,
						//and would make the upstream code flow exit gracefully.
						nullPadLen := MYSQLBINLOG_CHUNK - written
						emptyBuf := make([]byte, MYSQLBINLOG_CHUNK)
						_, err = writer.Write(emptyBuf[0:nullPadLen])
						if err != nil {
							//relog.Warning("Error in writing pad bytes to vt_mysqlbinlog %v", err)
							panic(fmt.Errorf("Error in writing pad bytes to vt_mysqlbinlog %v", err))
						}
						relog.Info("MAX_WAIT_TIMEOUT %v exceeded, closing connection", time.Duration(blr.MaxWaitTimeout*1e9))
						return
						//panic(fmt.Errorf("MAX_WAIT_TIMEOUT %v exceeded, closing connection", time.Duration(blr.MaxWaitTimeout*1e9)))
					}
				} else {
					positionWaitStart[position] = now
				}
			}
		}
	}
}

func copyBufN(dst io.Writer, src io.Reader, totalLen int64, buf []byte) (written int64, err error) {
	for written < totalLen {
		toBeRead := totalLen
		if diffLen := totalLen - written; diffLen < toBeRead {
			toBeRead = diffLen
		}
		nr, er := src.Read(buf[0:toBeRead])
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				relog.Warning("Short write to dst")
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			err = er
			break
		}
	}
	return written, err
}
