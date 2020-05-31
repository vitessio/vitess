# Instructions

Detailed instructions for running this example can be found at https://vitess.io.
This document contains the summary of the commands to be run.


```
# Edit main_vschema.json and set region_map to full path of countries.json file
# Example:
	    "region_map": "/home/user/vitess/examples/region_sharding/countries.json",


# Bring up initial cluster and main keyspace
./101_initial_cluster.sh

# Insert and verify data
mysql < insert_customers.sql
mysql --table < show_data.sql

# Down cluster
./201_teardown.sh
```
