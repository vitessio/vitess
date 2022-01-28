package vreplication

// The product, customer, Lead, Lead-1 tables are used to exercise and test most Workflow variants.
// We violate the NO_ZERO_DATES and NO_ZERO_IN_DATE sql_modes that are enabled by default in
// MySQL 5.7+ and MariaDB 10.2+ to ensure that vreplication still works everywhere and the
// permissive sql_mode now used in vreplication causes no unwanted side effects.
// The Lead and Lead-1 tables also allows us to test several things:
//   1. Mixed case identifiers
//   2. Column and table names with special characters in them, namely a dash
//   3. Identifiers using reserved words, as lead is a reserved word in MySQL 8.0+ (https://dev.mysql.com/doc/refman/8.0/en/keywords.html)
var (
	initialProductSchema = `
create table product(pid int, description varbinary(128), date1 datetime not null default '0000-00-00 00:00:00', date2 datetime not null default '2021-00-01 00:00:00', primary key(pid));
create table customer(cid int, name varbinary(128), meta json default null, typ enum('individual','soho','enterprise'), sport set('football','cricket','baseball'),
	ts timestamp not null default current_timestamp, bits bit(2) default b'11', date1 datetime not null default '0000-00-00 00:00:00', date2 datetime not null default '2021-00-01 00:00:00', primary key(cid)) CHARSET=utf8mb4;
create table customer_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
create table merchant(mname varchar(128), category varchar(128), primary key(mname)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
create table orders(oid int, cid int, pid int, mname varchar(128), price int, qty int, total int as (qty * price), total2 int as (qty * price) stored, primary key(oid));
create table order_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
create table customer2(cid int, name varbinary(128), typ enum('individual','soho','enterprise'), sport set('football','cricket','baseball'),ts timestamp not null default current_timestamp, primary key(cid));
create table customer_seq2(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
create table ` + "`Lead`(`Lead-id`" + ` binary(16), name varbinary(16), date1 datetime not null default '0000-00-00 00:00:00', date2 datetime not null default '2021-00-01 00:00:00', primary key (` + "`Lead-id`" + `));
create table ` + "`Lead-1`(`Lead`" + ` binary(16), name varbinary(16), date1 datetime not null default '0000-00-00 00:00:00', date2 datetime not null default '2021-00-01 00:00:00', primary key (` + "`Lead`" + `));
create table _vt_PURGE_1f9194b43b2011eb8a0104ed332e05c2_20201210194431(id int, val varbinary(128), primary key(id));
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
	},
	"Lead": {},
	"Lead-1": {}
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
	    },
		"bmd5": {
          "type": "binary_md5"
		}
	  },
   "tables":  {
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
	    },
	  "Lead": {
          "column_vindexes": [
	        {
	          "column": "Lead-id",
	          "name": "bmd5"
	        }
	      ]
		},
	  "Lead-1": {
          "column_vindexes": [
	        {
	          "column": "Lead",
	          "name": "bmd5"
	        }
	      ]
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
		"create_ddl": "create table cproduct(pid bigint, description varchar(128), date1 datetime not null default '0000-00-00 00:00:00', date2 datetime not null default '2021-00-01 00:00:00', primary key(pid))"
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

	// the merchant-type keyspace allows us to test keyspace names with special characters in them (dash)
	materializeMerchantOrdersSpec = `
{
  "workflow": "morders",
  "sourceKeyspace": "customer",
  "targetKeyspace": "merchant-type",
  "tableSettings": [{
    "targetTable": "morders",
    "sourceExpression": "select oid, cid, mname, pid, price, qty, total from orders",
    "create_ddl": "create table morders(oid int, cid int, mname varchar(128), pid int, price int, qty int, total int, total2 int as (10 * total), primary key(oid))"
  }]
}
`

	materializeMerchantSalesSpec = `
{
  "workflow": "msales",
  "sourceKeyspace": "customer",
  "targetKeyspace": "merchant-type",
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
