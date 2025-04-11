#!/bin/bash
set -x
vtexplain --vschema-file atomicity_vschema.json --schema-file atomicity_schema.sql --shards 4 --sql 'SET transaction_mode="single"; INSERT INTO t1 (c1) values (10),(14),(15),(16);'
