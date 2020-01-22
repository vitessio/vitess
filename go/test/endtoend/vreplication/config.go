package vreplication

var (
	initialProductSchema = `
create table product(pid int, description varbinary(128), primary key(pid));
create table customer(cid int, name varbinary(128),	primary key(cid));
create table merchant(mname varchar(128), category varchar(128), primary key(mname));
create table orders(oid int, cid int, pid int, mname varchar(128), price int, primary key(oid));
create table customer_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
`

	initialProductVSchema = `
{
  "tables": {
	"product": {},
	"customer": {},
	"merchant": {},
	"orders": {}
  }
}
`
	customerSchema  = "create table customer_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';"
	customerVSchema = `
{
  "sharded": false,
  "tables": {
    "customer":{}
   }
 }
`
	/*
	 "vindexes": {
	    "reverse_bits": {
	      "type": "reverse_bits"
	    }
	  },
	  "tables": {
	    "customer": {
	      "column_vindexes": [
	        {
	          "column": "cid",
	          "name": "reverse_bits"
	        }
	      ],
	      "auto_increment": {
	        "column": "cid",
	        "sequence": "customer_seq"
	      }
	    }
	  }

	*/
	merchantVSchema = `
{
  "sharded": true,
  "vindexes": {
    "md5": {
      "type": "unicode_loose_md5"
    }
  },
  "tables": {
    "merchant": {
      "column_vindexes": [
        {
          "column": "mname",
          "name": "md5"
        }
      ]
    }
  }
}
`
)
