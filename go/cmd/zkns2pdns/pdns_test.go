package main

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"code.google.com/p/vitess/go/netutil"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

const (
	fakeSRV = `{
"Entries": [
  {
    "host": "test1",
    "named_port_map": {"_http":8080},
    "ipv4": "0.0.0.1"
  },
  {
    "host": "test2",
    "named_port_map": {"_http":8080},
    "ipv4": "0.0.0.2"
  }
]}`

	fakeCNAME = `{
"Entries": [
  {
    "host": "test1"
  }
]}`

	fakeA = `{
"Entries": [
  {
    "host": "test1",
    "ipv4": "0.0.0.1"
  }
]}`
)

var fqdn = netutil.FullyQualifiedHostnameOrPanic()

var zconn = &TestZkConn{map[string]string{
	"/zk/test/zkns/srv":   fakeSRV,
	"/zk/test/zkns/cname": fakeCNAME,
	"/zk/test/zkns/a":     fakeA,
}}

var queries = []string{
	"Q\t_http.srv.zkns.test.zk\tIN\tANY\t-1\t1.1.1.1\t1.1.1.2",
	"Q\ta.zkns.test.zk\tIN\tANY\t-1\t1.1.1.1\t1.1.1.2",
	"Q\tcname.zkns.test.zk\tIN\tANY\t-1\t1.1.1.1\t1.1.1.2",
	"Q\tempty.zkns.test.zk\tIN\tANY\t-1\t1.1.1.1\t1.1.1.2",
	"Q\bad.domain\tIN\tANY\t-1\t1.1.1.1\t1.1.1.2",
}

var results = []string{
	"OK\tzkns2pdns\nDATA\t_http.srv.zkns.test.zk\tIN\tSRV\t1\t1\t0\t0 8080 test1\nDATA\t_http.srv.zkns.test.zk\tIN\tSRV\t1\t1\t0\t0 8080 test2\nDATA\t_http.srv.zkns.test.zk\tIN\tSOA\t1\t1\t" + fqdn + " hostmaster@" + fqdn + " 0 1800 600 3600 300\nEND\n",
	"OK\tzkns2pdns\nDATA\ta.zkns.test.zk\tIN\tA\t1\t1\t0.0.0.1\nDATA\ta.zkns.test.zk\tIN\tSOA\t1\t1\t" + fqdn + " hostmaster@" + fqdn + " 0 1800 600 3600 300\nDATA\ta.zkns.test.zk\tIN\tCNAME\t1\t1\ttest1\nEND\n",
	"OK\tzkns2pdns\nDATA\tcname.zkns.test.zk\tIN\tSOA\t1\t1\t" + fqdn + " hostmaster@" + fqdn + " 0 1800 600 3600 300\nDATA\tcname.zkns.test.zk\tIN\tCNAME\t1\t1\ttest1\nEND\n",
	"OK\tzkns2pdns\nDATA\tempty.zkns.test.zk\tIN\tSOA\t1\t1\t" + fqdn + " hostmaster@" + fqdn + " 0 1800 600 3600 300\nEND\n",
	"OK\tzkns2pdns\nFAIL\n",
}

func testQuery(t *testing.T, query, result string) {
	inpr, inpw, err := os.Pipe()
	if err != nil {
		panic(err)
	}
	defer inpr.Close()
	outpr, outpw, err := os.Pipe()
	if err != nil {
		inpw.Close()
		panic(err)
	}
	defer outpr.Close()

	zr1 := newZknsResolver(zconn, fqdn, ".zkns.test.zk", "/zk/test/zkns")
	pd := &pdns{zr1}
	go func() {
		pd.Serve(inpr, outpw)
		outpw.Close()
	}()

	_, err = io.WriteString(inpw, "HELO\t2\n")
	if err != nil {
		inpw.Close()
		t.Fatalf("write failed: %v", err)
	}
	_, err = io.WriteString(inpw, query)
	if err != nil {
		inpw.Close()
		t.Fatalf("write failed: %v", err)
	}

	inpw.Close()
	data, err := ioutil.ReadAll(outpr)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	qresult := string(data)
	if qresult != result {
		t.Fatalf("data mismatch: found %#v expected %#v", qresult, result)
	}
}

func TestQueries(t *testing.T) {
	for i, q := range queries {
		testQuery(t, q, results[i])
	}
}

// FIXME(msolomon) move to zk/fake package
type TestZkConn struct {
	data map[string]string
}

func (conn *TestZkConn) Get(path string) (data string, stat zk.Stat, err error) {
	data, ok := conn.data[path]
	if !ok {
		err = &zookeeper.Error{Op: "TestZkConn: node doesn't exist", Code: zookeeper.ZNONODE, Path: path}
		return
	}
	s := &zk.ZkStat{}
	return data, s, nil
}

func (conn *TestZkConn) GetW(path string) (data string, stat zk.Stat, watch <-chan zookeeper.Event, err error) {
	panic("Should not be used")
}

func (conn *TestZkConn) Children(path string) (children []string, stat zk.Stat, err error) {
	panic("Should not be used")
}

func (conn *TestZkConn) ChildrenW(path string) (children []string, stat zk.Stat, watch <-chan zookeeper.Event, err error) {
	panic("Should not be used")
}

func (conn *TestZkConn) Exists(path string) (stat zk.Stat, err error) {
	_, ok := conn.data[path]
	if ok {
		return &zk.ZkStat{}, nil
	}
	return nil, nil
}

func (conn *TestZkConn) ExistsW(path string) (stat zk.Stat, watch <-chan zookeeper.Event, err error) {
	panic("Should not be used")
}

func (conn *TestZkConn) Create(path, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	panic("Should not be used")
}

func (conn *TestZkConn) Set(path, value string, version int) (stat zk.Stat, err error) {
	panic("Should not be used")
}

func (conn *TestZkConn) Delete(path string, version int) (err error) {
	panic("Should not be used")
}

func (conn *TestZkConn) Close() error {
	panic("Should not be used")
}

func (conn *TestZkConn) RetryChange(path string, flags int, acl []zookeeper.ACL, changeFunc zk.ChangeFunc) error {
	panic("Should not be used")
}

func (conn *TestZkConn) ACL(path string) ([]zookeeper.ACL, zk.Stat, error) {
	panic("Should not be used")
}

func (conn *TestZkConn) SetACL(path string, aclv []zookeeper.ACL, version int) error {
	panic("Should not be used")
}
