/*
 * Copyright 2017 Google Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.example;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.UnsignedLong;
import io.vitess.client.Context;
import io.vitess.client.RpcClient;
import io.vitess.client.VTGateBlockingConnection;
import io.vitess.client.VTSession;
import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.Row;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.proto.Query.ExecuteOptions;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Map;
import java.util.Random;

/**
 * VitessClientExample.java is a sample for using the Vitess low-level Java Client.
 * <p>
 * Before running this, start up a local example cluster as described in the
 * examples/local/README.md file.
 */
public class VitessClientExample {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("usage: VitessClientExample <vtgate-host:port>");
            System.exit(1);
        }

        Context ctx = Context.getDefault().withDeadlineAfter(Duration.millis(5 * 1000));
        try (RpcClient client = new GrpcClientFactory().create(ctx, args[0]);
             VTGateBlockingConnection conn = new VTGateBlockingConnection(client)) {
            VTSession session = new VTSession("@master", ExecuteOptions.getDefaultInstance());
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

                conn.execute(ctx, "begin", null, session);
                conn.execute(
                        ctx,
                        "INSERT INTO messages (page,time_created_ns,message) VALUES (:page,:time_created_ns,:message)",
                        bindVars, session);
                conn.execute(ctx, "commit", null, session);
            }

            // Read it back from the master.
            System.out.println("Reading from master...");
            try (Cursor cursor =
                         conn.execute(
                                 ctx,
                                 "SELECT page, time_created_ns, message FROM messages",
                                 null, session)) {
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
