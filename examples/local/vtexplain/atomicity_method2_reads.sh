#!/bin/bash
set -x
vtexplain -vschema-file vschema.json -schema-file schema.sql -shards 4 -sql 'SET transaction_mode="single"; BEGIN; SELECT * from t1; COMMIT;'
