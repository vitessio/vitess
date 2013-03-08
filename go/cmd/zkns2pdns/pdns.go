package main

// To be used with PowerDNS (pdns) as a "pipe backend" CoProcess.
// Protocol description:
// http://downloads.powerdns.com/documentation/html/backends-detail.html#PIPEBACKEND.

import (
	"bufio"
	"bytes"
	"code.google.com/p/vitess/go/relog"
	"errors"
	"fmt"
	"io"
	"os"
)

var GREETING_ABI_V2 = []byte("HELO\t2")
var GREETING_REPLY = []byte("OK\tzkns2pdns\n")
var END_REPLY = []byte("END\n")
var FAIL_REPLY = []byte("FAIL\n")

const (
	KIND_AXFR = "AXFR" // ignored for now
	KIND_Q    = "Q"
	KIND_PING = "PING"
)

var (
	errLongLine = errors.New("pdns line too long")
	errBadLine  = errors.New("pdns line unparseable")
)

type pdnsConn struct {
	rwc io.ReadWriteCloser
}

type pdnsReq struct {
	kind     string
	qname    string
	qclass   string // always "IN"
	qtype    string // almost always "ANY"
	id       string
	remoteIp string
	localIp  string
}

type pdnsReply struct {
	qname   string
	qclass  string
	qtype   string
	ttl     string
	id      string
	content string // may be other tab-separated data based on SRV/MX records
}

func (pr *pdnsReply) fmtReply() string {
	return fmt.Sprintf("DATA\t%v\t%v\t%v\t%v\t%v\t%v\n", pr.qname, pr.qclass, pr.qtype, pr.ttl, pr.id, pr.content)
}

func parseReq(line []byte) (*pdnsReq, error) {
	tokens := bytes.Split(line, []byte("\t"))
	kind := string(tokens[0])
	switch kind {
	case KIND_Q:
		if len(tokens) < 7 {
			return nil, errBadLine
		}
		return &pdnsReq{kind, string(tokens[1]), string(tokens[2]), string(tokens[3]), string(tokens[4]), string(tokens[5]), string(tokens[6])}, nil
	case KIND_PING, KIND_AXFR:
		return &pdnsReq{kind: kind}, nil
	default:
		return nil, errBadLine
	}
	panic("unreachable")
}

// PDNS will query for "ANY" no matter what record type the client
// has asked for. Thus, we need to return data for all record
// types. PDNS will then filter for what the client needs.  PDNS is
// sensitive to the order in which records are returned.  If you
// return a CNAME first, it returns the CNAME for all queries.
// The DNS spec says you should not have conflicts between
// CNAME/SRV records, so this really shouldn't be an issue.
func handleQReq(req *pdnsReq) (lines []string, err error) {
	qtypes := []string{"SRV", "A", "SOA", "CNAME"}
	if req.qtype != "ANY" {
		qtypes = []string{req.qtype}
	}
	lines = make([]string, 0, 16)
	for _, qtype := range qtypes {
		replies, err := getResult(qtype, req.qname)
		if err != nil {
			relog.Error("query failed %v %v: %v", qtype, req.qname, err)
			continue
		}
		for _, reply := range replies {
			lines = append(lines, reply.fmtReply())
		}
	}
	if len(lines) == 0 {
		relog.Warning("no results for %v %v", req.qtype, req.qname)
	}
	return lines, nil
}

func getResult(qtype, qname string) ([]*pdnsReply, error) {
	return nil, nil
}

func (pdc *pdnsConn) Serve(r io.Reader, w io.Writer) {
	bufr := bufio.NewReader(r)
	needHandshake := true
	for {
		line, isPrefix, err := bufr.ReadLine()
		if err == nil && isPrefix {
			err = errLongLine
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			relog.Errorf("failed reading request: %v", err)
			continue
		}

		if needHandshake {
			if !bytes.Equal(line, GREETING_ABI_V2) {
				relog.Errorf("handshake failed: %v != %v", line, GREETING_ABI_V2)
				_, err = w.Write(FAIL_REPLY)
			} else {
				needHandshake = false
				_, err = w.Write(GREETING_REPLY)
			}
			if err != nil {
				relog.Errorf("failed writing reply: %v", err)
			}
			continue
		}

		req, err := parseReq(line)
		if err != nil {
			relog.Errorf("failed reading request: %v", err)
			_, err = w.Write(FAIL_REPLY)
			if err != nil {
				relog.Errorf("failed writing reply: %v", err)
			}
			continue
		}

		switch req.kind {
		case KIND_Q:
			respLines, err := handleQReq(req)
			if err != nil {
				relog.Errorf("failed query: %v %v", req.qname, err)
				_, err = w.Write(FAIL_REPLY)
				if err != nil {
					relog.Errorf("failed writing reply: %v", err)
				}
				continue
			}
			for _, line := range respLines {
				_, err = io.WriteString(w, line)
				if err != nil {
					relog.Errorf("failed writing reply: %v", err)
				}
			}
		case KIND_AXFR:
			// FIXME(mike) unimplemented
		}
		_, err = w.Write(END_REPLY)
		if err != nil {
			relog.Errorf("failed writing reply: %v", err)
		}
	}
}

func main() {
	new(pdnsConn).Serve(os.Stdin, os.Stdout)
}
