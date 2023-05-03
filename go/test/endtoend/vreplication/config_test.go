/*
Copyright 2022 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vreplication

// The product, customer, Lead, Lead-1 tables are used to exercise and test most Workflow variants.
// We violate the NO_ZERO_DATES and NO_ZERO_IN_DATE sql_modes that are enabled by default in
// MySQL 5.7+ and MariaDB 10.2+ to ensure that vreplication still works everywhere and the
// permissive sql_mode now used in vreplication causes no unwanted side effects.
// The customer table also tests two important things:
//  1. Composite or multi-column primary keys
//  2. PKs that contain an ENUM column
//
// The Lead and Lead-1 tables also allows us to test several things:
//  1. Mixed case identifiers
//  2. Column and table names with special characters in them, namely a dash
//  3. Identifiers using reserved words, as lead is a reserved word in MySQL 8.0+ (https://dev.mysql.com/doc/refman/8.0/en/keywords.html)
//
// The internal table _vt_PURGE_4f9194b43b2011eb8a0104ed332e05c2_20221210194431 should be ignored by vreplication
// The db_order_test table is used to ensure vreplication and vdiff work well with complex non-integer PKs, even across DB versions.
// The db_order_test table needs to use a collation that exists in all versions for cross version tests as we use the collation for the PK
// based merge sort in VDiff. The table is using a non-default collation for any version with utf8mb4 as 5.7 does NOT show the default
// collation in the SHOW CREATE TABLE output which means in the cross version tests the source and target will be using a different collation.
// The vdiff_order table is used to test MySQL sort->VDiff merge sort ordering and ensure it aligns across Reshards. It must not use the
// default collation as it has to work across versions and the 8.0 default does not exist in 5.7.
var (
	// All standard user tables should have a primary key and at least one secondary key.
	initialProductSchema = `
create table product(pid int, description varbinary(128), date1 datetime not null default '0000-00-00 00:00:00', date2 datetime not null default '2021-00-01 00:00:00', primary key(pid), key(date1,date2)) CHARSET=utf8mb4;
create table customer(cid int, name varchar(128) collate utf8mb4_bin, meta json default null, typ enum('individual','soho','enterprise'), sport set('football','cricket','baseball'),
	ts timestamp not null default current_timestamp, bits bit(2) default b'11', date1 datetime not null default '0000-00-00 00:00:00', 
	date2 datetime not null default '2021-00-01 00:00:00', dec80 decimal(8,0), blb blob, primary key(cid,typ), key(name)) CHARSET=utf8mb4;
create table customer_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
create table merchant(mname varchar(128), category varchar(128), primary key(mname), key(category)) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
create table orders(oid int, cid int, pid int, mname varchar(128), price int, qty int, total int as (qty * price), total2 int as (qty * price) stored, primary key(oid), key(pid), key(cid)) CHARSET=utf8;
create table order_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
create table customer2(cid int, name varchar(128), typ enum('individual','soho','enterprise'), sport set('football','cricket','baseball'),ts timestamp not null default current_timestamp, primary key(cid), key(ts)) CHARSET=utf8;
create table customer_seq2(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
create table ` + "`Lead`(`Lead-id`" + ` binary(16), name varbinary(16), date1 datetime not null default '0000-00-00 00:00:00', date2 datetime not null default '2021-00-01 00:00:00', primary key (` + "`Lead-id`" + `), key (date1));
create table ` + "`Lead-1`(`Lead`" + ` binary(16), name varbinary(16), date1 datetime not null default '0000-00-00 00:00:00', date2 datetime not null default '2021-00-01 00:00:00', primary key (` + "`Lead`" + `), key (date2));
create table _vt_PURGE_4f9194b43b2011eb8a0104ed332e05c2_20221210194431(id int, val varbinary(128), primary key(id), key(val));
create table db_order_test (c_uuid varchar(64) not null default '', created_at datetime not null, dstuff varchar(128), dtstuff text, dbstuff blob, cstuff char(32), primary key (c_uuid,created_at), key (dstuff)) CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
create table vdiff_order (order_id varchar(50) collate utf8mb4_unicode_ci not null, primary key (order_id), key (order_id)) charset=utf8mb4 COLLATE=utf8mb4_unicode_ci;
create table datze (id int, dt1 datetime not null default current_timestamp, dt2 datetime not null, ts1 timestamp default current_timestamp, primary key (id), key (dt1));
create table json_tbl (id int, j1 json, j2 json, primary key(id));
create table geom_tbl (id int, g geometry, p point, ls linestring, pg polygon, mp multipoint, mls multilinestring, mpg multipolygon, gc geometrycollection, primary key(id));
create table blob_tbl (id int, val1 varchar(20), blb1 blob, val2 varbinary(20), blb2 longblob, txt1 text, blb3 tinyblob, txt2 longtext, blb4 mediumblob, primary key(id));
`
	// These should always be ignored in vreplication
	internalSchema = `
 create table _1e275eef_3b20_11eb_a38f_04ed332e05c2_20201210204529_gho(id int, val varbinary(128), primary key(id));
 create table _0e8a27c8_1d73_11ec_a579_0aa0c75a6a1d_20210924200735_vrepl(id int, val varbinary(128), primary key(id));
 create table _vt_PURGE_1f9194b43b2011eb8a0104ed332e05c2_20201210194431(id int, val varbinary(128), primary key(id));
 create table _vt_EVAC_6ace8bcef73211ea87e9f875a4d24e90_29990915120410(id int, val varbinary(128), primary key(id));
 create table _vt_DROP_2bce8bcef73211ea87e9f875a4d24e90_20200915120410(id int, val varbinary(128), primary key(id));
 create table _vt_HOLD_4abe6bcef73211ea87e9f875a4d24e90_20220115120410(id int, val varbinary(128), primary key(id));
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
	"Lead-1": {},
	"db_order_test": {},
	"vdiff_order": {},
	"datze": {}
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
    "xxhash": {
      "type": "xxhash"
    },
    "unicode_loose_md5": {
      "type": "unicode_loose_md5"
    },
    "bmd5": {
      "type": "binary_md5"
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
    },
    "db_order_test": {
      "column_vindexes": [
        {
          "columns": ["c_uuid", "created_at"],
          "name": "xxhash"
        }
      ]
    },
    "vdiff_order": {
      "column_vindexes": [
        {
          "column": "order_id",
          "name": "unicode_loose_md5"
        }
      ]
    },
    "geom_tbl": {
      "column_vindexes": [
         {
           "column": "id",
           "name": "reverse_bits"
         }
       ]
    },
    "json_tbl": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "reverse_bits"
        }
      ]
    },
    "blob_tbl": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "reverse_bits"
        }
      ]
    },
    "datze": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "reverse_bits"
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
    },
    "unicode_loose_md5": {
      "type": "unicode_loose_md5"
    },
    "xxhash": {
      "type": "xxhash"
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
    "db_order_test": {
      "column_vindexes": [
        {
          "columns": ["c_uuid", "created_at"],
          "name": "xxhash"
        }
      ]
    },
    "vdiff_order": {
      "column_vindexes": [
        {
          "column": "order_id",
          "name": "unicode_loose_md5"
        }
      ]
    },
    "geom_tbl": {
      "column_vindexes": [
         {
           "column": "id",
           "name": "reverse_bits"
         }
       ]
    },
    "json_tbl": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "reverse_bits"
        }
      ]
    },
    "blob_tbl": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "reverse_bits"
        }
      ]
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
		"create_ddl": "create table cproduct(pid bigint, description varchar(128), date1 datetime not null default '0000-00-00 00:00:00', date2 datetime not null default '2021-00-01 00:00:00', primary key(pid)) CHARSET=utf8mb4"
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
    "create_ddl": "create table morders(oid int, cid int, mname varchar(128), pid int, price int, qty int, total int, total2 int as (10 * total), primary key(oid)) CHARSET=utf8"
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
    "create_ddl": "create table msales(merchant_name varchar(128), kount int, amount int, primary key(merchant_name)) CHARSET=utf8"
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
    "create_ddl": "create table sales(pid int, kount int, amount int, primary key(pid)) CHARSET=utf8"
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
    "create_ddl": "create table rollup(rollupname varchar(100), kount int, primary key (rollupname)) CHARSET=utf8mb4"
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

	jsonValues = []string{
		`"abc"`,
		`123`,
		`{"foo": 456}`,
		`{"bar": "foo"}`,
		`[1, "abc", 932409834098324908234092834092834, 234234234234234234234234.2342342342349]`,
		`{"a":2947293482093480923840923840923, "cba":334234234234234234234234234.234234239090}`,
		`[1, "abc", -1, 0.2342342342349, {"a":"b","c":"d","ab":"abc","bc":["x","y"]}]`,
		`{"a":2947293482093480923840923840923, "cba":{"a":2947293482093480923840923840923, "cba":334234234234234234234234234.234234239090}}`,
		`{"asdf":{"foo":123}}`,
		`{"a":"b","c":"d","ab":"abc","bc":["x","y"]}`,
		`["here",["I","am"],"!!!"]`,
		`{"scopes":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAEAAAAAAEAAAAAA8AAABgAAAAAABAAAACAAAAAAAAA"}`,
		`"scalar string"`,
		`"scalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar stringscalar string"`,
		`"first line\\r\\nsecond line\\rline with escapes\\\\ \\r\\n"`,
		`true`,
		`false`,
		`""`,
		`-1`,
		`1`,
		`32767`,
		`32768`,
		`-32768`,
		`-32769`,
		`2.147483647e+09`,
		`1.8446744073709552e+19`,
		`-9.223372036854776e+18`,
		`{}`,
		`[]`,
		`"2015-01-15 23:24:25.000000"`,
		`"23:24:25.000000"`,
		`"23:24:25.120000"`,
		`"2015-01-15"`,
	}
)
