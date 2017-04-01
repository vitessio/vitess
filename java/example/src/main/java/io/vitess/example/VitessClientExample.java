package io.vitess.example;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.UnsignedLong;
import io.vitess.client.Context;
import io.vitess.client.RpcClient;
import io.vitess.client.VTGateBlockingConn;
import io.vitess.client.VTGateBlockingTx;
import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.Row;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata.TabletType;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Random;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * VitessClientExample.java is a sample for using the Vitess low-level Java Client.
 *
 * Before running this, start up a local example cluster as described in the
 * examples/local/README.md file.
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
        VTGateBlockingConn conn = new VTGateBlockingConn(client)) {
      // Insert some messages on random pages.
      System.out.println("Inserting into master...");
      Random rand = new Random();
      for (int i = 0; i < 3; i++) {
        Instant timeCreated = Instant.now();
        Map<String, Object> bindVars =
            new ImmutableMap.Builder<String, Object>()
                .put("page", rand.nextInt(100) + 1)
                .put("time_created_ns", timeCreated.getMillis() * 1000000)
                .put("message", "V is for speed")
                .build();

        VTGateBlockingTx tx = conn.begin(ctx);
        tx.execute(
            ctx,
            "INSERT INTO messages (page,time_created_ns,message) VALUES (:page,:time_created_ns,:message)",
            bindVars,
            TabletType.MASTER,
            Query.ExecuteOptions.IncludedFields.ALL);
        tx.commit(ctx);
      }

      // Read it back from the master.
      System.out.println("Reading from master...");
      try (Cursor cursor =
          conn.execute(
              ctx,
              "SELECT page, time_created_ns, message FROM messages",
              null /* bindVars */,
              TabletType.MASTER,
              Query.ExecuteOptions.IncludedFields.ALL)) {
        Row row;
        while ((row = cursor.next()) != null) {
          UnsignedLong page = row.getULong("page");
          UnsignedLong timeCreated = row.getULong("time_created_ns");
          byte[] message = row.getBytes("message");
          System.out.format("(%s, %s, %s)\n", page, timeCreated, new String(message));
        }
      }
    } catch (Exception e) {
      System.out.println("Vitess Java example failed.");
      System.out.println("Error Details:");
      e.printStackTrace();
      System.exit(2);
    }
  }
}
