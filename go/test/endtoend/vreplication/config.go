package vreplication

var (
	initialProductSchema = `
create table product(pid int, description varbinary(128), primary key(pid));
create table customer(cid int, name varbinary(128), meta json default null, typ enum('individual','soho','enterprise'), sport set('football','cricket','baseball'),ts timestamp not null default current_timestamp, primary key(cid))  CHARSET=utf8mb4;
create table customer_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
create table merchant(mname varchar(128), category varchar(128), primary key(mname)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
create table orders(oid int, cid int, pid int, mname varchar(128), price int, primary key(oid));
create table order_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
create table customer2(cid int, name varbinary(128), typ enum('individual','soho','enterprise'), sport set('football','cricket','baseball'),ts timestamp not null default current_timestamp, primary key(cid));
create table customer_seq2(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
`

	initialProductVSchema = `
{
  "tables": {
	"product": {},
	"merchant": {},
	"orders": {},
	"customer": {},
	"customer_seq": {
		"type": "sequence"
	},
	"customer2": {},
	"customer_seq2": {
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
	    },
	    "customer2": {
	      "column_vindexes": [
	        {
	          "column": "cid",
	          "name": "reverse_bits"
	        }
	      ],
	      "auto_increment": {
	        "column": "cid",
	        "sequence": "customer_seq2"
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
	},
	"vproduct": {
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
			  "column": "merchant_name",
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
	"sourceExpression": "select mname as merchant_name, count(*) as kount, sum(price) as amount from orders group by merchant_name",
    "create_ddl": "create table msales(merchant_name varchar(128), kount int, amount int, primary key(merchant_name))"
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
	materializeRollupSpec = `
{
  "workflow": "rollup",
  "sourceKeyspace": "product",
  "targetKeyspace": "product",
  "tableSettings": [{
    "targetTable": "rollup",
    "sourceExpression": "select 'total' as rollupname, count(*) as kount from product group by rollupname",
    "create_ddl": "create table rollup(rollupname varchar(100), kount int, primary key (rollupname))"
  }]
}
`
	initialExternalSchema = `
create table review(rid int, pid int, review varbinary(128), primary key(rid));
create table rating(gid int, pid int, rating int, primary key(gid));
`

	initialExternalVSchema = `
{
  "tables": {
	"review": {},
	"rating": {}
  }
}
`
)
