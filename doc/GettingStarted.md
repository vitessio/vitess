You can build Vitess using either [Docker](#docker-build) or a
[manual](#manual-build) build process.

If you run into issues or have questions, please post on our
[forum](https://groups.google.com/forum/#!forum/vitess).

## Docker Build

To run Vitess in Docker, use an
[Automated Build](https://registry.hub.docker.com/repos/vitess/).

* The <code>vitess/base</code> image contains a full development
  environment, capable of building Vitess and running integration tests.
* The <code>vitess/lite</code> image contains only the compiled Vitess
  binaries, excluding ZooKeeper. it can run Vitess, but lacks the
  environment needed to build Vitess or run tests.

You can also build Vitess Docker images yourself to include your
own patches or configuration data. The
[Dockerfile](https://github.com/youtube/vitess/blob/master/Dockerfile)
in the root of the Vitess tree builds the <code>vitess/base</code> image.
The [docker](https://github.com/youtube/vitess/tree/master/docker)
subdirectory contains scripts for building other images.

## Manual Build

The following sections explain the process for manually building
Vitess on a local server:

-    [Install Dependencies](#install-dependencies)
-    [Build Vitess](#build-vitess)
-    [Test Your Vitess Cluster](#test-your-vitess-cluster)
-    [Start a Vitess Cluster](#start-a-vitess-cluster)
-    [Run a Client Application](#run-a-client-application)
-    [Tear Down the Cluster](#tear-down-the-cluster)

### Install Dependencies

Vitess runs on either Ubuntu 14.04 (Trusty) or Debian 7.0 (Wheezy). It requires the software and libraries listed below.

1.  [Install Go 1.3+](http://golang.org/doc/install).

2.  Install [MariaDB 10.0](https://downloads.mariadb.org/) or
    [MySQL 5.6](http://dev.mysql.com/downloads/mysql). You can use any
    installation method (src/bin/rpm/deb), but be sure to include the client
    development headers (**libmariadbclient-dev** or **libmysqlclient-dev**).
    <br><br>
    The Vitess development team currently tests against MariaDB 10.0.17
    and MySQL 5.6.24.
    <br><br>
    If you are installing MariaDB, note that you must install version 10.0 or
    higher. If you are using <code>apt-get</code>, confirm that your repository
    offers an option to install that version. You can also download the source
    directly from [mariadb.org](https://downloads.mariadb.org/mariadb/10.0.16/).

3.  Select a lock service from the options listed below. It is technically
    possible to use another lock server, but plugins currently exist only
    for ZooKeeper and etcd.
    - ZooKeeper 3.3.5 is included by default. 
    - [Install etcd 0.4.6](https://github.com/coreos/etcd/releases/tag/v0.4.6).
      If you use etcd, remember to include the <code>etcd</code> command
      on your path.


4.  Install the following other tools needed to build and run Vitess:
    - make
    - automake
    - libtool
    - memcached
    - python-dev
    - python-virtualenv
    - python-mysqldb
    - libssl-dev
    - g++
    - mercurial
    - git
    - pkg-config
    - bison
    - curl
    - unzip<br><br>

    These can be installed with the following apt-get command:

    ``` sh
sudo apt-get install make automake libtool memcached python-dev python-virtualenv python-mysqldb libssl-dev g++ mercurial git pkg-config bison curl unzip
```

5.  If you decided to use ZooKeeper in step 3, you also need to install a
    Java Runtime, such as OpenJDK.

    ``` sh
      sudo apt-get install openjdk-7-jre</pre>
```

### Build Vitess

1.  Navigate to the directory where you want to download the Vitess
    source code and clone the Vitess Github repo. After doing so,
    navigate to the **src/github.com/youtube/vitess** directory.

    ``` sh
cd $WORKSPACE
git clone https://github.com/youtube/vitess.git src/github.com/youtube/vitess
cd src/github.com/youtube/vitess
```

1.  Set the **MYSQL_FLAVOR** environment variable. Choose the appropriate
    value for your database. This value is case-sensitive.

    ``` sh
export MYSQL_FLAVOR=MariaDB
or
export MYSQL_FLAVOR=MySQL56
```

1.  If your selected database installed in a location other than **/usr/bin**,
    set the **VT&#95;MYSQL&#95;ROOT** variable to the root directory of your
    MariaDB installation. For example, if MariaDB is installed in
    **/usr/local/mysql**, run the following command.

    ``` sh
export VT_MYSQL_ROOT=/usr/local/mysql
```

    <br>Note that the command indicates that the **mysql** executable should
    be found at **/usr/local/mysql/bin/mysql**.

1.  Run <code>mysql_config --version</code> and confirm that you
    are running the correct version of MariaDB or MySQL. The value should
    be <code>10</code> or higher for MariaDB and 5.6.x for MySQL.

1.  Build Vitess using the commands below. Note that the
    <code>bootstrap.sh</code> script needs to download some dependencies.
    If your machine requires a proxy to access the Internet, you will need
    to set the usual environment variables (e.g. <code>http_proxy</code>,
    <code>https_proxy</code>, <code>no_proxy</code>).

    ``` sh
./bootstrap.sh
# Output of bootstrap.sh is shown below
# skipping zookeeper build
# go install golang.org/x/tools/cmd/cover ...
# Found MariaDB installation in ...
# skipping bson python build
# skipping cbson python build
# creating git pre-commit hooks
#
# source dev.env in your shell before building
# Remaining commands to build Vitess
. ./dev.env
make build
```

    <br>
    If you build Vitess successfully, you can proceed to
    [start a Vitess cluster](#start-a-vitess-cluster).
    If your build attempt fails, see the following section,
    [Test your Vitess Cluster](#test-your-vitess-cluster), for help
    troubleshooting the errors.

### Test your Vitess Cluster

**Note:** If you are using etcd, set the following environment variable:

``` sh
export VT_TEST_FLAGS='--topo-server-flavor=etcd'
```

The default _make_ and _make test_ targets contain a full set of tests
intended to help Vitess developers to verify code changes. Those tests
simulate a small Vitess cluster by launching many servers on the local
machine. To do so, they require a lot of resources; a minimum of 8GB RAM
and SSD is recommended to run the tests.

If you want only to check that Vitess is working in your environment,
you can run a lighter set of tests:

``` sh
make site_test
```

#### Common Test Issues

Attempts to run the full developer test suite (_make_ or _make test_)
on an underpowered machine often results in failure. If you still see
the same failures when running the lighter set of tests (*make site_test*),
please let the development team know in the
[vitess@googlegroups.com](https://groups.google.com/forum/#!forum/vitess)
discussion forum.

##### Node already exists, port in use, etc.

A failed test can leave orphaned processes. If you use the default
settings, you can use the following commands to identify and kill
those processes:

``` sh
pgrep -f -l '(vtdataroot|VTDATAROOT)' # list Vitess processes
pkill -f '(vtdataroot|VTDATAROOT)' # kill Vitess processes
```

##### Too many connections to MySQL, or other timeouts

This error often means your disk is too slow. If you don't have access
to an SSD, you can try [testing against a
ramdisk](https://github.com/youtube/vitess/blob/master/doc/TestingOnARamDisk.md).

##### Connection refused to tablet, MySQL socket not found, etc.

These errors might indicate that the machine ran out of RAM and a server
crashed when trying to allocate more RAM. Some of the heavier tests
require up to 8GB RAM.

##### Connection refused in zkctl test

This error might indicate that the machine does not have a Java Runtime
installed, which is a requirement if you are using ZooKeeper as the lock server.

##### Running out of disk space

Some of the larger tests use up to 4GB of temporary space on disk.


### Start a Vitess cluster

After completing the instructions above to [build Vitess](#build-vitess),
you can use the example scripts in the Github repo to bring up a Vitess
cluster on your local machine. These scripts use ZooKeeper as the
lock service. ZooKeeper is included in the Vitess distribution.

1.  **Configure environment variables**

    If you are still in the same terminal window that
    you used to run the build commands, you can skip to the next
    step since the environment variables will already be set.<br><br>

    Navigate to the directory where you built Vitess
    (**$WORKSPACE/src/github.com/youtube/vitess**) and run the
    following command:

    ``` sh
. ./dev.env
```

    <br>From that directory, navigate to the directory that contains
    the example scripts:

    ``` sh
cd examples/local
```

1.  **Start ZooKeeper**

    Servers in a Vitess cluster find each other by looking for
    dynamic configuration data stored in a distributed lock
    service. The following script creates a small ZooKeeper cluster:

    ``` sh
./zk-up.sh
# Starting zk servers...
# Waiting for zk servers to be ready...
```

    <br>
    After the ZooKeeper cluster is running, we only need to tell each
    Vitess process how to connect to ZooKeeper. Then, each process can
    find all of the other Vitess processes by coordinating via ZooKeeper.
    <br><br>
    To instruct Vitess processes on how to connect to ZooKeeper, set
    the <code>ZK_CLIENT_CONFIG</code> environment variable to the path
    to the <code>zk-client-conf.json</code> file, which contains the
    ZooKeeper server addresses for each cell.

1.  **Start vtctld**

    The <code>vtctld</code> server provides a web interface that
    displays all of the coordination information stored in ZooKeeper.

    ``` sh
./vtctld-up.sh
# Starting vtctld
# Access vtctld at http://localhost:15000
```

    <br>Open <code>http://localhost:15000</code> to verify that
    <code>vtctld</code> is running. There won't be any information
    there yet, but the menu should come up, which indicates that
    <code>vtctld</code> is running.<br><br>

    **Note:** The <code>vtctld</code> server accepts commands from
    the <code>vtctlclient</code> tool, which administers the Vitess
    cluster. The following command lists the commands that
    <code>vtctlclient</code> supports:

    ``` sh
$VTROOT/bin/vtctlclient -server localhost:15000
```

1.  **Start vttablets**

    The following command brings up three vttablets. Bringing up
    tablets in a previously empty keyspace effectively creates
    a new shard.

    ``` sh
./vttablet-up.sh
# Output from vttablet-up.sh is below
# Starting MySQL for tablet test-0000000100...
# Starting vttablet for test-0000000100...
# Access tablet test-0000000100 at http://localhost:15100/debug/status
# Starting MySQL for tablet test-0000000101...
# Starting vttablet for test-0000000101...
# Access tablet test-0000000101 at http://localhost:15101/debug/status
# Starting MySQL for tablet test-0000000102...
# Starting vttablet for test-0000000102...
# Access tablet test-0000000102 at http://localhost:15102/debug/status
```

    <br>After this command completes, go back to the <code>vtctld</code>
    web page from the previous step and click the **Topology** link.
    You should see the three tablets listed. If you click the address
    of a tablet, you will see the coordination data stored in ZooKeeper.
    From there, if you click the **status** link at the top of the page,
    you will see the debug page generated by the tablet itself.

1.  **Initialize a keyspace for the shard**

    Perform a keyspace rebuild to initialize the keyspace for the
    new shard:

    ``` sh
$VTROOT/bin/vtctlclient -server localhost:15000 RebuildKeyspaceGraph test_keyspace
```

    <br>
    **Note:** Most <code>vtctlclient</code> commands yield no output if
    they run successfully.

1.  **Elect a master vttablet**

    The <code>vttablet</code> servers are all started as replicas.
    In this step, you designate one of the vttablets to be the master.
    Vitess automatically connects the other replicas' <code>mysqld</code>
    instances so that they start replicating from the master
    <code>mysqld</code> instance.<br><br>

    Since this is the first time the shard has been started, the
    vttablets are not already doing any replication. As a result,
    the following command uses the <code>-force</code> flag when
    calling the <code>InitShardMaster</code> command so that it
    accepts the change of a replica to be the master.

    ``` sh
$VTROOT/bin/vtctlclient -server localhost:15000 InitShardMaster -force test_keyspace/0 test-0000000100
```

    <br>After running this command, go back to the **Topology** page
    in the <code>vtctld</code> web interface. When you refresh the
    page, you should see that one <code>vttablet</code> is the master
    and the other two are replicas.<br><br>

    You can also run this command on the command line to see the
    same data:

    ``` sh
$VTROOT/bin/vtctlclient -server localhost:15000 ListAllTablets test
# The command's output is shown below:
# test-0000000100 test_keyspace 0 master localhost:15100 localhost:33100 []
# test-0000000101 test_keyspace 0 replica localhost:15101 localhost:33101 []
# test-0000000102 test_keyspace 0 replica localhost:15102 localhost:33102 []
```

1.  **Create a table**

    Create the database table defined in the <i>createtesttable.sql</i>
    file.

    ``` sh
$VTROOT/bin/vtctlclient -server localhost:15000 ApplySchemaKeyspace -simple -sql "$(cat create_test_table.sql)" test_keyspace
```

    <br>The SQL to create the table is shown below:

    ``` sh
CREATE TABLE test_table (
  id BIGINT AUTO_INCREMENT,
  msg VARCHAR(250),
  PRIMARY KEY(id)
) Engine=InnoDB
```

1.  **Start vtgate**

    Vitess uses <code>vtgate</code> to route each client query to
    the correct <code>vttablet</code>. This local example runs a
    single <code>vtgate</code> instance, though a real deployment
    would likely run multiple <code>vtgate</code> instances to share
    the load.

    ``` sh
./vtgate-up.sh
```

### Run a Client Application

The <code>client.py</code> file is a simple sample application
that connects to <code>vtgate</code> and executes some queries.
To run it, you need to either:

1.  Add the Vitess Python packages to your <code>PYTHONPATH</code><br>
    or

1.  Use the <code>client.sh</code> wrapper script, which temporarily
    sets up the environment and then runs <code>client.py</code>

    ``` sh
./client.sh --server=localhost:15001
# Output from client.sh is shown below
# Inserting into master...
# Reading from master...
# (1L, 'V is for speed')
# Reading from replica...
# (1L, 'V is for speed')
```



### Tear down the cluster

As long as your <code>VTDATAROOT</code> directory is only used
for this test, you can kill the processes created during this
test with the following commands:

``` sh
# Look for processes started during the test
$ pgrep -u $USER -f -l $VTDATAROOT

# If the list looks correct, kill the processes
$ pkill -u $USER -f $VTDATAROOT
```

Repeat the <code>pgrep</code> command to make sure all processes
have been killed and manually kill any remaining processes.

Also clear out the contents of <code>VTDATAROOT</code> before
starting again:

``` sh
$ cd $VTDATAROOT
$ /path/to/vtdataroot$ rm -rf *
```
