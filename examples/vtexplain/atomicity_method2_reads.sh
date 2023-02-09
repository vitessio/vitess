#!/bin/bash
set -x
vtexplain --vschema-file atomicity_vschema.json --schema-file atomicity_schema.sql --shards 4 --sql 'SET transaction_mode="single"; BEGIN; SELECT * from t1; COMMIT;'
