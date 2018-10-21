## Does the application need to know about the sharding scheme underneath Vitess?

The application does not need to know about how the data is sharded. This information is stored in a VSchema which the VTGates use to automatically route your queries. This allows the application to connect to vitess and use it as if it’s a single giant database server.

## Can I address a specific shard if I want to?

If necessary, you can access a specific shard by connecting to it using the shard specific database name. For a keyspace `ks` and shard `-80`, you would connect to `ks:-80`.

## How do I choose between master vs. replica for queries?

You can qualify the keyspace name with the desired tablet type using the `@` suffix. This can be specified as part of the connection as the database name, or can be changed on the fly through the `USE` command.

For example, `ks@master` will select `ks` as the default keyspace with all queries being sent to the master. Consequently `ks@replica` will load balance requests across all `REPLICA` tablet types, and `ks@rdonly` will choose `RDONLY`.

You can also specify the database name as `@master`, etc, which instructs vitess that no default keyspace was specified, but that the requests are for the specified tablet type.

If no tablet type was specified, then VTGate chooses its default, which can be overridden with the `-default_tablet_type` command line argument.

## There seems to be a 10,000 rowcount limit per query. What if I want to do a full table scan?

Vitess supports different modes. In OLTP mode, the result size is typically limited to a preset number (10,000 rows by default). This limit can be adjusted based on your needs.

However, OLAP mode has no limit to the rowcount. In order to change to this mode, you may issue the following command command before executing your query:

```
set workload='olap'
```

You can also set the workload to `dba` mode, which allows you to override the implicit timeouts that exist in vttablet. However, this mode should be used judiciously as it supersedes shutdown and reparent commands.

The general convention is to send OLTP queries to `REPLICA` tablet types, and OLAP queries to `RDONLY`.

## Is there a list of supported/unsupported queries?

The list of unsupported constructs is currently in the form of test cases contained in this [test file](https://github.com/vitessio/vitess/blob/master/data/test/vtgate/unsupported_cases.txt). However, contrary to the test cases, there is limited support for SET, DDL and DBA constructs. This will be documented soon.


## If I have a log of all queries from my app. Is there a way I can try them against vitess to see how they’ll work?

Yes. The vtexplain tool can be used to preview how your queries will be executed by vitess. It can also be used to try different sharding scenarios before deciding on one.

## Does the Primary Vindex for a tablet have to be the same as its Primary Key.

It is not necessary that a Primary Vindex be the same as the Primary Key. In fact, there are many use cases where you would not want this. For example, if there are tables with one-to-many relationships, the Primary Vindex of the main table is likely to be the same as the Primary Key. However, if you want the rows of the secondary table to reside in the same shard as the parent row, the Primary Vindex for that table must be the foreign key that points to the main table.

## How do I connect to vtgate using mysql protocol?

If you look at the example [vtgate-up.sh script](https://github.com/vitessio/vitess/blob/master/examples/local/vtgate-up.sh), you'll see the following lines:


```
  -mysql_server_port $mysql_server_port \
  -mysql_server_socket_path $mysql_server_socket_path \
  -mysql_auth_server_static_file "./mysql_auth_server_static_creds.json" \
```

In that example, vtgate accepts mysql connections on port 15306, and the authentication info is stored in the json file. So, you should be able to connect to it using the following command:

```
mysql -h 127.0.0.1 -P 15306 -u mysql_user --password=mysql_password
```

## Can I override the default db name from `vt_xxx` to my own?

Yes. You can start vttablet with the `-init_db_name_override` command line option to specify a different db name.

## I cannot start a cluster, and see these errors in the logs: Could not open required defaults file: /path/to/my.cnf

Most likely this means that apparmor is running on your server and is preventing vitess processes from accessing the my.cnf file. The workaround is to uninstall apparmor:

```
sudo service apparmor stop
sudo service apparmor teardown
sudo update-rc.d -f apparmor remove
```

You may also need to reboot the machine after this. Many programs automatically install apparmor. Re-uninstall as needed.
