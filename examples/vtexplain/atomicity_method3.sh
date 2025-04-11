#!/bin/bash
set -x
# SET transaction_mode="multi"  is implied by default
vtexplain --vschema-file atomicity_vschema.json --schema-file atomicity_schema.sql --shards 4 --sql 'INSERT INTO t1 (c1) values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20);'
