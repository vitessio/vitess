# Support for Mysqldump

In order to export and import mysqldump through vtgate, unsupported queries and statements need to be stripped from the dump.  
A 2-step process is advised. 
1. Dump the schema only and import.
2. Dump the data and import.

## Export and Import mysqldump into Vitess

Define the variables then run the script
```
vitess/support/mysqldump$ ./mysqldump.sh
```
The script will output 2 files to enable debugging;  
`schema.sql`  
`data.sql`  
The script will also attempt to import the schema and data into Vitess via vtgate.


## Notes
This has been tested for small databases.  
The script can be modified to include other unsupported statements that may be present.  
The script has been tested with GNU sed, BSD sed users e.g. MacOS will need to update the script.  