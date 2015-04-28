This page explains how to start a Kubernetes cluster and also run
Vitess on Kubernetes. This example was most recently tested using
the binary release of Kubernetes v0.9.1.

## Prerequisites

To complete the exercise in this guide, you must locally install Go 1.3+,
Vitess' <code>vtctlclient</code> tool, and Google Cloud SDK. The
following sections explain how to set these up in your environment.

### Install Go 1.3+

You need to install [Go 1.3+](http://golang.org/doc/install) to build the
<code>vtctlclient</code> tool, which issues commands to Vitess.

After installing Go, make sure your <code>GOPATH</code> environment
variable is set to the root of your workspace. The most common setting
is <code>GOPATH=$HOME/go</code>, and the value should identify a
directory to which your non-root user has write access.

In addition, make sure that <code>$GOPATH/bin</code> is included in
your <code>$PATH</code>. More information about setting up a Go
workspace can be found at
[http://golang.org/doc/code.html#Organization]
(http://golang.org/doc/code.html#Organization).

## Build and install <code>vtctlclient</code>

The <code>vtctlclient</code> tool issues commands to Vitess.

``` sh
$ go get github.com/youtube/vitess/go/cmd/vtctlclient
```

This command downloads and builds the Vitess source code at:

``` sh
$GOPATH/src/github.com/youtube/vitess/
```

It also copies the built <code>vtctlclient</code> binary into <code>$GOPATH/bin</code>.

### Set up Google Compute Engine, Container Engine, and Cloud tools

To run Vitess on Kubernetes using Google Compute Engine (GCE),
you must have a GCE account with billing enabled. The instructions
below explain how to enable billing and how to associate a billing
account with a project in the Google Developers Console.

1.  Log in to the Google Developers Console to [enable billing]
    (https://console.developers.google.com/billing).
    1.  Click the **Billing** pane if you are not there already.
    1.  Click **New billing account**
    1.  Assign a name to the billing account -- e.g. "Vitess on
        Kubernetes." Then click **Continue**. You can sign up
        for the [free trial](https://cloud.google.com/free-trial/)
        to avoid any charges.

1.  Create a project in the Google Developers Console that uses
    your billing account:
    1.  In the Google Developers Console, click the **Projects** pane.
    1.  Click the Create Project button.
    1.  Assign a name to your project. Then click the **Create** button.
        Your project should be created and associated with your
        billing account. (If you have multiple billing accounts,
        confirm that the project is associated with the correct account.)
    1.  After creating your project, click **APIs & auth** in the left menu.
    1.  Click **APIs**.
    1.  Find **Google Compute Engine** and **Google Container Engine API**
        and click the **OFF** button for each to enable those two APIs.

1.  Follow the [GCE quickstart guide]
    (https://cloud.google.com/compute/docs/quickstart#setup) to set up
    and test the Google Cloud SDK. You will also set your default project
    ID while completing the quickstart. Start with step 2 in the setup
    process.

    **Note:** During the quickstart, you'll generate an SSH key for
    Google Compute Engine, and you will be prompted to enter a
    passphrase. You will be prompted for that passphrase several times
    when bringing up your Kubernetes cluster later in this guide.

## Start a Kubernetes cluster

1.  Set the <code>KUBECTL</code> environment variable to point to the
    <code>gcloud</code> command:

    ``` sh
$ export KUBECTL='gcloud alpha container kubectl'
```

1.  Enable alpha features in the <code>gcloud</code> tool:

    ``` sh
$ gcloud components update alpha
```

1.  If you did not complete the [GCE quickstart guide]
    (https://cloud.google.com/compute/docs/quickstart#setup), set
    your default project ID by running the following command.
    Replace <code>PROJECT</code> with the project ID assigned to your
    [Google Developers Console](https://console.developers.google.com/)
    project. You can [find the ID]
    (https://cloud.google.com/compute/docs/overview#projectids)
    by navigating to the **Overview** page for the project
    in the Console.

    ``` sh
$ gcloud config set project PROJECT
```

1.  Set the [zone](https://cloud.google.com/compute/docs/zones#overview)
    that your installation will use:

    ``` sh
$ gcloud config set compute/zone us-central1-b
```

1.  Create a Kubernetes cluster:

    ``` sh
$ gcloud alpha container clusters create example --machine-type n1-standard-1 --num-nodes 3
```

1.  While the cluster is starting, you will be prompted several
    times for the passphrase you created while setting up Google
    Compute Engine.

1.  The command's output includes the URL for the Kubernetes master server:

    ``` sh
endpoint: 146.148.70.28
masterAuth:
  password: YOUR_PASSWORD
  user: admin
```

    1.  Open the endpoint URL in a browser to get the full effect
        of the "Hello World" experience in Kubernetes.

    1.  If you see a <code>ERRCERTAUTHORITY_INVALID</code> error
        indicating that the server's security certificate is not
        trusted by your computer's operating system, click the
        **Advanced** link and then the link to proceed to the URL.

    1.  You should be prompted to enter a username and password to
        access the requested page. Enter the <code>masterAuth</code>
        username and password from the <code>gcloud</code> command's
        output.


## Start a Vitess cluster

1.  **Navigate to your local Vitess source code**

    This directory would have been created when you installed
    <code>vtctlclient</code>:

    ``` sh
$ cd $GOPATH/src/github.com/youtube/vitess
```

1.  **Start an etcd cluster:**

    ``` sh
$ cd examples/kubernetes
vitess/examples/kubernetes$ ./etcd-up.sh
```

    <br>This command creates two clusters. One is for the global cell,
    and the other is for the test cell. You can check the status
    of the [pods]
    (https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/pods.md)
    in the cluster by running:

    ``` sh
$ $KUBECTL get pods
```

    <br>It may take a while for each Kubernetes minion to download the
    Docker images the first time it needs them. While the images
    are downloading, the pod status will be Pending.<br><br>

    **Note:** In this example, each script that has a name ending in
    <code>-up.sh</code> also has a corresponding <code>-down.sh</code>
    script, which can be used to stop certain components of the
    Vitess cluster without bringing down the whole cluster. For
    example, to tear down the <code>etcd</code> deployment, run:

    ``` sh
vitess/examples/kubernetes$ ./etcd-down.sh
```

1.  **Start vtctld**

    The <code>vtctld</code> server provides a web interface to
    inspect the state of the Vitess cluster. It also accepts RPC
    commands from <code>vtctlclient</code> to modify the cluster.

    ``` sh
vitess/examples/kubernetes$ ./vtctld-up.sh
```

    <br>To let you access <code>vtctld</code> from outside Kubernetes,
    the <code>vtctld</code> service is created with the
    <code>createExternalLoadBalancer</code> option. This is a
    [convenient shortcut]
    (https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/services.md#external-services)
    for cloud providers that support external load balancers.
    On supported platforms, Kubernetes will then automatically
    create an external IP that load balances onto the pods
    comprising the service.<br><br>

1.  **Access vtctld**

    To access the <code>vtctld</code> service from outside
    Kubernetes, you need to open port 15000 on the GCE firewall.
    (If you don't complete this step, the only way to issue commands
    to <code>vtctld</code> would be to SSH into a Kubernetes node
    and install and run <code>vtctlclient</code> there.)

    ``` sh
$ gcloud compute firewall-rules create vtctld --allow tcp:15000
```

    <br>Then, get the address of the load balancer for <code>vtctld</code>:

    ``` sh
$ gcloud compute forwarding-rules list
NAME   REGION      IP_ADDRESS    IP_PROTOCOL TARGET
vtctld us-central1 104.154.64.12 TCP         us-central1/targetPools/vtctld
```

    <br>You can then access the <code>vtctld</code> web interface
    at port 15000 of the IP address returned in the above command.
    In this example, the web UI would be at
    <code>https://104.154.64.12:15000</code>.

1.  **Use <code>vtctlclient</code> to call <code>vtctld</code>**

    You can now run <code>vtctlclient</code> locally to issue commands
    to the <code>vtctld</code> service on your Kubernetes cluster.<br><br>

    When you call <code>vtctlclient</code>, the command includes
    the IP address and port for your <code>vtctld</code> service.
    To avoid having to enter that for each command, create an alias
    called <code>kvtctl</code> that points to the address from above:

    ``` sh
$ alias kvtctl='vtctlclient -server 104.154.64.12:15000'
```

    <br>Now, running <code>kvtctl</code> will test your connection to
    <code>vtctld</code> and also list the <code>vtctlclient</code>
    commands that you can use to administer the Vitess cluster.

    ``` sh
# Test the connection to vtctld and list available commands
$ kvtctl help
No command specified please see the list below:
Tablets:
  InitTablet ...
  ...
```

1.  **Start vttablets**

    Call the following script to launch <code>vttablet</code>
    and <code>mysqld</code> in a [pod]
    (https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/pods.md):

    ``` sh
vitess/examples/kubernetes$ ./vttablet-up.sh
### Output from vttablet-up.sh is shown below
# Creating test_keyspace.shard-0 pods in cell test...
# Creating pod for tablet test-0000000100...
# vttablet-100
#
# Creating pod for tablet test-0000000101...
# vttablet-101
#
# Creating pod for tablet test-0000000102...
# vttablet-102
```

    <br>Wait until you see the tablets listed in the
    **DBTopology Tool** summary page for your <code>vtctld</code>
    instance. This can take some time if a pod was scheduled on a
    minion that needs to download the latest Vitess Docker image.
    You can also check the status of the tablets from the command
    line using <code>kvtctl</code>.

    ``` sh
$ kvtctl ListAllTablets test
```

    <br>By bringing up tablets in a previously empty keyspace, you
    have effectively just created a new shard. To initialize the
    keyspace for the new shard, call the
    <code>vtctl RebuildKeyspaceGraph</code> command:

    ``` sh
$ kvtctl RebuildKeyspaceGraph test_keyspace
```

    <br>After this command completes, go back to the <code>vtctld</code>
    UI and click the **DBTopology Tool** link. You should see the
    three tablets listed. If you click the address of a tablet, you
    will see the coordination data stored in <code>etcd</code>.<br><br>

    **Note:** Most <code>vtctlclient</code> commands produce no
    output on success.<br><br>

    **_Status pages for vttablets_**

    Each <code>vttablet</code> serves a set of HTML status pages
    on its primary port. The <code>vtctld</code> interface provides
    a link to the status page for each tablet, but the links are
    actually to internal, per-pod IPs that can only be accessed
    from within Kubernetes.<br><br>

    As such, if you try to connect to one of the **[status]**
    links, you will get a 502 HTTP response.<br><br>

    As a workaround, you can proxy over an SSH connection to a
    Kubernetes minion, or you can launch a proxy as a Kubernetes
    service. In the future, we plan to provide proxying via the
    Kubernetes API server without a need for additional setup. 

1.  **Elect a master vttablet**

    The vttablets are all started as replicas. In this step, you
    designate one of the vttablets to be the master. Vitess
    automatically connects the other replicas' mysqld instances
    so that they start replicating from the master's mysqld.<br><br>

    Since this is the first time the shard has been started,
    the vttablets are not already doing any replication, and the
    tablet types are all replica or spare. As a
    result, the following command uses the <code>-force</code>
    flag when calling the <code>InitShardMaster</code> command
    to be able to promote one instance to master.


    ``` sh
$ kvtctl InitShardMaster -force test_keyspace/0 test-0000000100
```

    <br>**Note:** If you do not include the <code>-force</code> flag 
    here, the command will first check to ensure the provided
    tablet is the only tablet of type master in the shard.
    However, since none of the slaves are masters, and we're not
    replicating at all, that check would fail and the command
    would fail as well.<br><br>

    After running this command, go back to the **DBTopology Tool**
    in the <code>vtctld</code> web interface. When you refresh the
    page, you should see that one <code>vttablet</code> is the master
    and the other two are replicas.<br><br>

    You can also run this command on the command line to see the
    same data:

    ``` sh
$ kvtctl ListAllTablets test
# The command's output is shown below:
# test-0000000100 test_keyspace 0 master MASTER_IP:15002 MASTER_IP:3306 []
# test-0000000101 test_keyspace 0 replica REPLICA_IP:15002 REPLICA_IP:3306 []
# test-0000000102 test_keyspace 0 replica REPLICA_IP:15002 REPLICA_IP:3306 []
```

1.  **Create a table**

    The <code>vtctlclient</code> tool implements the database schema
    across all tablets in a keyspace. The following command creates
    the table defined in the _createtesttable.sql_ file:

    ``` sh
vitess/examples/kubernetes$ kvtctl ApplySchemaKeyspace -simple -sql "$(cat create_test_table.sql)" test_keyspace
```

    <br>The SQL to create the table is shown below:

    ``` sh
CREATE TABLE test_table (
  id BIGINT AUTO_INCREMENT,
  msg VARCHAR(250),
  PRIMARY KEY(id)
) Engine=InnoDB
```

    <br>You can run this command to confirm that the schema was created
    properly on a given tablet, where <code>test-0000000100</code>
    is a tablet ID as listed in step 4 or step 7:

    ``` sh
kvtctl GetSchema test-0000000100
```

1.  **Start <code>vtgate</code>**

    Vitess uses <code>vtgate</code> to route each client query
    to the correct <code>vttablet</code>. In Kubernetes, a
    <code>vtgate</code> service distributes connections to a pool
    of <code>vtgate</code> pods. The pods are curated by a [replication
    controller](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/replication-controller.md).

    ``` sh
vitess/examples/kubernetes$ ./vtgate-up.sh
```

## Test your instance with a client app

The GuestBook app in the example is ported from the [Kubernetes GuestBook example](https://github.com/GoogleCloudPlatform/kubernetes/tree/master/examples/guestbook-go). The server-side code has been rewritten in Python to use Vitess as the storage engine. The client-side code (HTML/JavaScript) is essentially unchanged.

``` sh
vitess/examples/kubernetes$ ./guestbook-up.sh
```

As with the <code>vtctld</code> service, to access the GuestBook
app from outside Kubernetes, you need to open a port (3000) on
your firewall.

``` sh
# Open port 3000 in the firewall
$ gcloud compute firewall-rules create guestbook --allow tcp:3000
```

Then, get the external IP of the load balancer for the GuestBook service:

``` sh
$ gcloud compute forwarding-rules list
NAME      REGION      IP_ADDRESS     IP_PROTOCOL TARGET
guestbook us-central1 146.148.72.125 TCP         us-central1/targetPools/guestbook
vtctld    us-central1 104.154.64.12  TCP         us-central1/targetPools/vtctld
```

Once the pods are running, the GuestBook app should be accessible
from port 3000 on the external IP.

You can see Vitess' replication capabilities by opening the app in
multiple browser windows. Each new entry is committed to the master
database. In the meantime, JavaScript on the page continuously polls
the app server to retrieve a list of GuestBook entries. The app serves
read-only requests by querying Vitess in 'replica' mode, confirming
that replication is working.

The [GuestBook source code]
(https://github.com/youtube/vitess/tree/master/examples/kubernetes/guestbook)
provides more detail about how the app server interacts with Vitess.

## Tear down and clean up

The following command tears down the Container Engine cluster. It is
necessary to stop the virtual machines running on the Cloud platform.

``` sh
$ gcloud alpha container clusters delete example
```

And these commands clean up other entities created for this example.
They are suggested to prevent conflicts that might occur if you
don't run them and then rerun this example in a different mode.

``` sh
$ gcloud compute forwarding-rules delete k8s-example-default-vtctld
$ gcloud compute forwarding-rules delete k8s-example-default-guestbook
$ gcloud compute firewall-rules delete vtctld
$ gcloud compute firewall-rules delete guestbook
$ gcloud compute target-pools delete k8s-example-default-vtctld
$ gcloud compute target-pools delete k8s-example-default-guestbook
```

## Troubleshooting

If a pod enters the <code>Running</code> state, but the server
doesn't respond as expected, use the <code>kubectl log</code>
command to check the pod output:

``` sh
# show logs for container 'vttablet' within pod 'vttablet-100'
$ $KUBECTL log vttablet-100 vttablet

# show logs for container 'mysql' within pod 'vttablet-100'
$ $KUBECTL log vttablet-100 mysql
```

Post the logs somewhere and send a link to the [Vitess
mailing list](https://groups.google.com/forum/#!forum/vitess)
to get more help.
