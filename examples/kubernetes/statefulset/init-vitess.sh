#!/bin/bash

set -x

./kvtctl.sh InitShardMaster -force main/-80 zone1-1504789100
./kvtctl.sh InitShardMaster -force main/80- zone1-1015307700
./kvtctl.sh ApplySchema -sql "$(cat schema.sql)" main
./kvtctl.sh ApplyVSchema -vschema "$(cat vschema.json)" main
./kvtctl.sh Backup zone1-1504789101
./kvtctl.sh Backup zone1-1015307701
