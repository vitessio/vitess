package vreplication

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var (
	initialSchema = `
create table product(
	pid int,
	description varbinary(128),
	primary key(pid)
);
create table customer(
	cid int,
	name varbinary(128),
	primary key(cid)
);
create table merchant(
	mname varchar(128),
	category varchar(128),
	primary key(mname)
);
create table orders(
	oid int,
	cid int,
	pid int,
	mname varchar(128),
	price int,
	primary key(oid)
);`
	initialVSchema = `
{
  "tables": {
	"product": {},
	"customer": {},
	"merchant": {},
	"orders": {}
  }
}
`
)


func TestBasicVreplicationWorkflow (t *testing.T) {
	// TODO remove Setenv block below
	// This TEMPORARY HACK is required because GoLand is not using my env variables and defining them in Run/Edit Configurations did not work :(

	err := os.Setenv("PATH", "/home/rohit/vitess-ps/dist/etcd/etcd-v3.3.10-linux-amd64:/home/rohit/vitess-ps/bin:/home/rohit/vitess-ps/dist/etcd/etcd-v3.3.10-linux-amd64:/home/rohit/vitess-ps/bin:/usr/local/go/bin:/home/rohit/vitess/dist/etcd/etcd-v3.3.10-linux-amd64:/home/rohit/vitess/bin:/home/rohit/vitess-ps/dist/etcd/etcd-v3.3.10-linux-amd64:/home/rohit/vitess-ps/bin:/usr/local/go/bin:/home/rohit/vitess/dist/etcd/etcd-v3.3.10-linux-amd64:/home/rohit/vitess/bin:/home/rohit/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin")
	if err != nil {
		fmt.Printf("err %v\n", err)
	}
	err = os.Setenv("VTDATAROOT", "/home/rohit/vtdataroot")
	if err != nil {
		fmt.Printf("err %v\n", err)
	}
	err = os.Setenv("VTROOT", "/home/rohit/vitess-ps/")
	if err != nil {
		fmt.Printf("err %v\n", err)
	}

	vc := InitCluster(t, "zone1")
	if false {  //TODO use the keep-data flag
		defer vc.TearDown()
	}

	assert.NotNil(t, vc)
	vc.AddKeyspace(t, vc.Cells["zone1"], "product", "0", initialVSchema, initialSchema, 1, 0) //TODO doesn't work with numReplicas = 0

	//Initial cluster setup here, now testing various vreplication functionality

}
