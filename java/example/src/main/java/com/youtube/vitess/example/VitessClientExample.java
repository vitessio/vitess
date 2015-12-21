package com.youtube.vitess.example;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;

import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.RpcClient;
import com.youtube.vitess.client.VTGateConn;
import com.youtube.vitess.client.VTGateTx;
import com.youtube.vitess.client.cursor.Cursor;
import com.youtube.vitess.client.cursor.Row;
import com.youtube.vitess.client.grpc.GrpcClientFactory;
import com.youtube.vitess.proto.Topodata.TabletType;

import org.joda.time.Duration;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;

/**
 * VitessClientExample.java is a sample for using the Vitess Java Client with an unsharded keyspace.
 *
 * Before running this, start up a local example cluster as described in the
 * examples/local/README.md file.
 *
 * Alternatively, load the schema examples/local/create_test_table.sql into your instance:
 *
 *   $VTROOT/bin/vtctlclient -server <vtctld-host:port> ApplySchema -sql \
 *   "$(cat create_test_table.sql)" test_keyspace
 */
public class VitessClientExample {
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("usage: VitessClientExample <vtgate-host:port>");
      System.exit(1);
    }

    // Connect to vtgate.
    HostAndPort hostAndPort = HostAndPort.fromString(args[0]);
    Context ctx = Context.getDefault().withDeadlineAfter(Duration.millis(5 * 1000));
    RpcClient client = new GrpcClientFactory().create(
        ctx, new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPort()));
    VTGateConn conn = new VTGateConn(client);

    String keyspace = "test_keyspace";
    Iterable<String> shards = Arrays.asList("0");
    Map<String, Object> bindVars =
        new ImmutableMap.Builder<String, Object>().put("msg", "V is for speed").build();

    // Insert something.
    System.out.println("Inserting into master...");
    VTGateTx tx = conn.begin(ctx);
    tx.executeShards(ctx, "INSERT INTO test_table (msg) VALUES (:msg)", keyspace, shards, bindVars,
        TabletType.MASTER,
        /* notInTransaction (ignore this because we will soon remove it) */ false);
    tx.commit(ctx);

    // Read it back from the master.
    System.out.println("Reading from master...");
    Cursor cursor = conn.executeShards(ctx, "SELECT id, msg FROM test_table", keyspace, shards,
        /* bindVars */ null, TabletType.MASTER);
    Row row;
    while ((row = cursor.next()) != null) {
      long id = row.getLong("id");
      byte[] msg = row.getBytes("msg");
      System.out.format("(%d, %s)\n", id, new String(msg));
    }
    cursor.close();

    conn.close();
    client.close();
  }
}
