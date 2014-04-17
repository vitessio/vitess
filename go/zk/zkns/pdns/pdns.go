// To be used with PowerDNS (pdns) as a "pipe backend" CoProcess.
//
// Protocol description:
// http://downloads.powerdns.com/documentation/html/backends-detail.html#PIPEBACKEND.
//
// Mainly the resolver has to interpret zkns addresses in a way that
// is helpful for DNS. This involves approximating CNAME/A records
// when appropriate.
package pdns

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/zk"
	"github.com/youtube/vitess/go/zk/zkns"
)

var (
	GREETING_ABI_V2 = []byte("HELO\t2")
	GREETING_REPLY  = "OK\tzkns2pdns\n"
	END_REPLY       = "END\n"
	FAIL_REPLY      = "FAIL\n"
)

var (
	errLongLine = errors.New("pdns line too long")
	errBadLine  = errors.New("pdns line unparseable")
)

const (
	KIND_AXFR = "AXFR" // ignored for now
	KIND_Q    = "Q"
	KIND_PING = "PING"
)

const (
	defaultTTL      = "1"
	defaultId       = "1"
	defaultPriority = 0
	defaultWeight   = 0
)

type zknsResolver struct {
	zconn      zk.Conn
	fqdn       string // The fqdn of this machine.
	zknsDomain string // The chunk of naming hierarchy to serve.
	zkRoot     string // The root path from which to resolve.
}

func newZknsResolver(zconn zk.Conn, fqdn, zknsDomain, zkRoot string) *zknsResolver {
	if fqdn[len(fqdn)-1] == '.' {
		fqdn = fqdn[:len(fqdn)-1]
	}
	if zknsDomain[len(zknsDomain)-1] == '.' {
		zknsDomain = zknsDomain[:len(zknsDomain)-1]
	}
	if zknsDomain[0] != '.' {
		zknsDomain = "." + zknsDomain
	}
	return &zknsResolver{zconn, fqdn, zknsDomain, zkRoot}
}

func (rz *zknsResolver) getResult(qtype, qname string) ([]*pdnsReply, error) {
	if !strings.HasSuffix(qname, rz.zknsDomain) {
		return nil, fmt.Errorf("invalid domain for query: %v", qname)
	}

	switch qtype {
	case "SOA":
		// primary hostmaster serial refresh retry expire default_ttl
		content := fmt.Sprintf("%v. hostmaster.%v. 0 1800 600 3600 300", rz.fqdn, rz.fqdn)
		return []*pdnsReply{&pdnsReply{rz.zknsDomain + ".", "IN", qtype, defaultTTL, defaultId, content}}, nil
	case "SRV":
		return rz.getSRV(qname)
	case "CNAME":
		return rz.getCNAME(qname)
	case "A":
		reply, error := rz.getA(qname)
		// An A query must also check for a CNAME, but not vice versa
		if reply == nil && error == nil {
			return rz.getCNAME(qname)
		}
		return reply, error
	}
	return nil, nil
}

// Reverse a slice in place. Return the same slice for convenience.
func reverse(p []string) []string {
	i := 0
	j := len(p) - 1
	for i < j {
		p[i], p[j] = p[j], p[i]
		i++
		j = len(p) - i - 1
	}
	return p
}

func (rz *zknsResolver) getSRV(qname string) ([]*pdnsReply, error) {
	if !strings.HasSuffix(qname, rz.zknsDomain) {
		return nil, fmt.Errorf("invalid domain for query: %v", qname)
	}
	zkname := qname[:len(qname)-len(rz.zknsDomain)]
	nameParts := strings.Split(zkname, ".")
	portName := nameParts[0]
	if portName[0] != '_' {
		// Since PDNS probes for all types, this isn't really an error worth mentioning.
		// fmt.Errorf("invalid port name for query: %v", portName)
		log.Infof("skipping SRV query: %v", qname)
		return nil, nil
	}
	nameParts = reverse(nameParts[1:])

	zkPath := path.Join(rz.zkRoot, path.Join(nameParts...))
	addrs, err := zkns.ReadAddrs(rz.zconn, zkPath)
	if err != nil {
		return nil, err
	}

	if !addrs.IsValidSRV() {
		log.Warningf("invalid SRV record addrs at path %v", zkPath)
		// Since PDNS probes for all types, this isn't really an error worth propagating.
		// return nil, fmt.Errorf("invalid response for CNAME query: %v", qname)
		return nil, nil
	}

	replies := make([]*pdnsReply, 0, 16)
	for _, addr := range addrs.Entries {
		content := fmt.Sprintf("%v\t%v %v %v", defaultPriority, defaultWeight, addr.NamedPortMap[portName], addr.Host)
		replies = append(replies, &pdnsReply{qname, "IN", "SRV", defaultTTL, defaultId, content})
	}
	return replies, nil
}

// An CNAME record is generated when there is only one ZknsAddr, it
// has no port component. Multiple CNAMEs violate DNS protocols despite
// support in some versions of BIND.
func (rz *zknsResolver) getCNAME(qname string) ([]*pdnsReply, error) {
	if !strings.HasSuffix(qname, rz.zknsDomain) {
		return nil, fmt.Errorf("invalid domain for query: %v", qname)
	}
	if qname[0] == '_' {
		// Since PDNS probes for all types, use some heuristics to limit error noise.
		log.Infof("skipping CNAME query: %v", qname)
		return nil, nil
	}
	zkname := qname[:len(qname)-len(rz.zknsDomain)]
	nameParts := reverse(strings.Split(zkname, "."))
	zkPath := path.Join(rz.zkRoot, path.Join(nameParts...))
	addrs, err := zkns.ReadAddrs(rz.zconn, zkPath)
	if err != nil {
		return nil, err
	}

	if !addrs.IsValidCNAME() {
		log.Warningf("invalid CNAME record addrs at path %v", zkPath)
		// Since PDNS probes for all types, this isn't really an error worth propagating.
		// return nil, fmt.Errorf("invalid response for CNAME query: %v", qname)
		return nil, nil
	}

	return []*pdnsReply{&pdnsReply{qname, "IN", "CNAME", defaultTTL, defaultId, addrs.Entries[0].Host}}, nil
}

// An A record is generated when there is only one ZknsAddr, it
// has no port component and provides an IPv4 address.
// FIXME(msolo) Multiple IP addresses could be handed out.
func (rz *zknsResolver) getA(qname string) ([]*pdnsReply, error) {
	if !strings.HasSuffix(qname, rz.zknsDomain) {
		return nil, fmt.Errorf("invalid domain for query: %v", qname)
	}
	if qname[0] == '_' {
		// Since PDNS probes for all types, use some heuristics to limit error noise.
		log.Infof("skipping A query, looks like SRV name: %v", qname)
		return nil, nil
	}
	zkname := qname[:len(qname)-len(rz.zknsDomain)]
	nameParts := reverse(strings.Split(zkname, "."))
	zkPath := path.Join(rz.zkRoot, path.Join(nameParts...))
	addrs, err := zkns.ReadAddrs(rz.zconn, zkPath)
	if err != nil {
		return nil, err
	}

	if !addrs.IsValidA() {
		log.Warningf("invalid A record addrs at path %v", zkPath)
		// Since PDNS probes for all types, this isn't really an error worth propagating.
		// return nil, fmt.Errorf("invalid response for A query: %v", qname)
		return nil, nil
	}

	replies := make([]*pdnsReply, len(addrs.Entries))
	for i, entry := range addrs.Entries {
		replies[i] = &pdnsReply{qname, "IN", "A", defaultTTL, defaultId, entry.IPv4}
	}
	// Shuffle replies since that seems like the only reasonable strategy in a
	// stateless distributed environment.
	for i := range replies {
		j := rand.Intn(i + 1)
		replies[i], replies[j] = replies[j], replies[i]
	}
	return replies, nil
}

type pdns struct {
	zr *zknsResolver
}

// NewPdnsResolver creates a new PdnsResolver.
func NewPdnsResolver(zconn zk.Conn, fqdn, zknsDomain, zkRoot string) *pdns {
	zr1 := newZknsResolver(zconn, fqdn, zknsDomain, zkRoot)
	return &pdns{zr1}
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
			log.Errorf("bad token count for line: %s", line)
			return nil, errBadLine
		}
		return &pdnsReq{kind, string(tokens[1]), string(tokens[2]), string(tokens[3]), string(tokens[4]), string(tokens[5]), string(tokens[6])}, nil
	case KIND_PING, KIND_AXFR:
		return &pdnsReq{kind: kind}, nil
	default:
		log.Errorf("bad kind for line: %s", line)
		return nil, errBadLine
	}
}

// PDNS will query for "ANY" no matter what record type the client
// has asked for. Thus, we need to return data for all record
// types. PDNS will then filter for what the client needs.  PDNS is
// sensitive to the order in which records are returned.  If you
// return a CNAME first, it returns the CNAME for all queries.
// The DNS spec says you should not have conflicts between
// CNAME/SRV records, so this really shouldn't be an issue.
func (pd *pdns) handleQReq(req *pdnsReq) (lines []string, err error) {
	qtypes := []string{"SOA", "SRV", "A", "CNAME"}
	if req.qtype != "ANY" {
		qtypes = []string{req.qtype}
	}
	lines = make([]string, 0, 16)
	for _, qtype := range qtypes {
		replies, err := pd.zr.getResult(qtype, req.qname)
		if err != nil {
			// If we aren't even the authority, we might as well give up.
			if qtype == "SOA" {
				return nil, err
			}
			log.Errorf("query failed %v %v: %v", qtype, req.qname, err)
			continue
		}
		for _, reply := range replies {
			lines = append(lines, reply.fmtReply())
		}
	}
	if len(lines) == 0 {
		emptyCount.Add(1)
		log.Warningf("no results for %v %v", req.qtype, req.qname)
	}
	return lines, nil
}

func write(w io.Writer, line string) {
	_, err := io.WriteString(w, line)
	if err != nil {
		log.Errorf("write failed: %v", err)
	}
}

var (
	requestCount = stats.NewInt("PdnsRequestCount")
	errorCount   = stats.NewInt("PdnsErrorCount")
	emptyCount   = stats.NewInt("PdnsEmptyCount")
)

// Serve runs the PDNS server protol on the given reader/writer.
func (pd *pdns) Serve(r io.Reader, w io.Writer) {
	log.Infof("starting zkns resolver")
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
			log.Errorf("failed reading request: %v", err)
			continue
		}

		if needHandshake {
			if !bytes.Equal(line, GREETING_ABI_V2) {
				log.Errorf("handshake failed: %v != %v", line, GREETING_ABI_V2)
				write(w, FAIL_REPLY)
			} else {
				needHandshake = false
				write(w, GREETING_REPLY)
			}
			continue
		}

		requestCount.Add(1)
		req, err := parseReq(line)
		if err != nil {
			errorCount.Add(1)
			log.Errorf("failed parsing request: %v", err)
			write(w, FAIL_REPLY)
			continue
		}

		switch req.kind {
		case KIND_Q:
			respLines, err := pd.handleQReq(req)
			if err != nil {
				errorCount.Add(1)
				log.Errorf("failed query: %v %v", req.qname, err)
				write(w, FAIL_REPLY)
				continue
			}
			for _, line := range respLines {
				write(w, line)
			}
		case KIND_AXFR:
			// FIXME(mike) unimplemented
		}
		write(w, END_REPLY)
	}
}
