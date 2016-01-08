package com.youtube.vitess.example;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;

import org.joda.time.Duration;

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

/**
 * VitessClientExample.java is a sample for using the Vitess Java Client with an unsharded keyspace.
 *
 * Before running this, start up a local example cluster as described in the
 * examples/local/README.md file.
 *
 * Alternatively, load the schema examples/local/create_test_table.sql into your instance:
 *
 * <pre>
 *   $VTROOT/bin/vtctlclient -server <vtctld-host:port> ApplySchema -sql \
 *   "$(cat create_test_table.sql)" test_keyspace
 * </pre>
 */
public class VitessClientExample {
  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("usage: VitessClientExample <vtgate-host:port>");
      System.exit(1);
    }

    // Connect to vtgate.
    HostAndPort hostAndPort = HostAndPort.fromString(args[0]);
    InetSocketAddress addr =
        new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPort());
    Context ctx = Context.getDefault().withDeadlineAfter(Duration.millis(5 * 1000));
    try (RpcClient client = new GrpcClientFactory().create(ctx, addr);
        VTGateConn conn = new VTGateConn(client);) {
      String keyspace = "test_keyspace";
      Iterable<String> shards = Arrays.asList("0");
      Map<String, Object> bindVars =
          new ImmutableMap.Builder<String, Object>().put("msg", "V is for speed").build();

      // Insert something.
      System.out.println("Inserting into master...");
      VTGateTx tx = conn.begin(ctx);
      tx.executeShards(ctx, "INSERT INTO test_table (msg) VALUES (:msg)", keyspace, shards,
          bindVars, TabletType.MASTER, false /* notInTransaction (ignore this because we will soon remove it) */);
      tx.commit(ctx);

      // Read it back from the master.
      System.out.println("Reading from master...");
      try (Cursor cursor =
          conn.executeShards(ctx, "SELECT id, msg FROM test_table", keyspace, shards,
              null /* bindVars */, TabletType.MASTER);) {
        Row row;
        while ((row = cursor.next()) != null) {
          long id = row.getLong("id");
          byte[] msg = row.getBytes("msg");
          System.out.format("(%d, %s)\n", id, new String(msg));
        }
      }
    } catch (Exception e) {
      System.out.println("Vitess Java example failed.");
      System.out.println("Error Details:");
      e.printStackTrace();
    }
  }
}
