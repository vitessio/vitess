package vreplication

var (
	initialProductSchema = `
create table product(pid int, description varbinary(128), primary key(pid));
create table customer(cid int, name varbinary(128),	primary key(cid));
create table merchant(mname varchar(128), category varchar(128), primary key(mname));
create table orders(oid int, cid int, pid int, mname varchar(128), price int, primary key(oid));
create table customer_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
create table order_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
`

	initialProductVSchema = `
{
  "tables": {
	"product": {},
	"customer": {},
	"merchant": {},
	"orders": {},
	"customer_seq": {
		"type": "sequence"
	},
	"order_seq": {
		"type": "sequence"
	}
  }
}
`
	customerSchema  = ""
	customerVSchema = `
{
  "sharded": true,
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
  
}
`
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
	//ordersSchema = "create table order_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';"
	ordersVSchema = `
{
  "sharded": true,
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
	},
    "orders": {
      "column_vindexes": [
        {
          "column": "oid",
          "name": "reverse_bits"
        }
      ],
      "auto_increment": {
        "column": "oid",
        "sequence": "order_seq"
      }

    }
  }
}
`
	materializeProductVSchema = `
{
  "sharded": true,
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
	},
    "orders": {
      "column_vindexes": [
        {
          "column": "oid",
          "name": "reverse_bits"
        }
      ],
      "auto_increment": {
        "column": "oid",
        "sequence": "order_seq"
      }

    },
	"cproduct": {
		"type": "reference"
	}
  }
}
`
	materializeProductSpec = `
	{
	"workflow": "cproduct",
	"sourceKeyspace": "product",
	"targetKeyspace": "customer",
	"tableSettings": [{
		"targetTable": "cproduct",
		"sourceExpression": "select * from product",
		"create_ddl": "create table cproduct(pid bigint, description varchar(128), primary key(pid))"
	}]
}
`

	merchantOrdersVSchema = `
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
      	},
	  	"morders": {
		  "column_vindexes": [
			{
			  "column": "mname",
			  "name": "md5"
			}
		  ],
		  "auto_increment": {
			"column": "oid",
			"sequence": "order_seq"
		  }
	  	},
	  	"msales": {
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
	materializeMerchantOrdersSpec = `
{
  "workflow": "morders",
  "sourceKeyspace": "customer",
  "targetKeyspace": "merchant",
  "tableSettings": [{
    "targetTable": "morders",
    "sourceExpression": "select * from orders",
    "create_ddl": "create table morders(oid int, cid int, mname varchar(128), pid int, price int, primary key(oid))"
  }]
}
`
	materializeMerchantSalesSpec = `
{
  "workflow": "msales",
  "sourceKeyspace": "customer",
  "targetKeyspace": "merchant",
  "tableSettings": [{
    "targetTable": "msales",
    "sourceExpression": "select mname, count(*) as kount, sum(price) as amount from orders group by mname",
    "create_ddl": "create table msales(mname varchar(128), kount int, amount int, primary key(mname))"
  }]
}
`

	materializeSalesVSchema = `
{
  "tables": {
    "product": {},
    "sales": {},
    "customer_seq": {
      "type": "sequence"
    },
    "order_seq": {
      "type": "sequence"
    }
  }
}
`
	materializeSalesSpec = `
{
  "workflow": "sales",
  "sourceKeyspace": "customer",
  "targetKeyspace": "product",
  "tableSettings": [{
    "targetTable": "sales",
    "sourceExpression": "select pid, count(*) as kount, sum(price) as amount from orders group by pid",
    "create_ddl": "create table sales(pid int, kount int, amount int, primary key(pid))"
  }]
}
`
)
