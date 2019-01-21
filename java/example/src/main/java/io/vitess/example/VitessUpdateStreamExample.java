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

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.joda.time.Duration;

import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Queues;

import io.grpc.StatusRuntimeException;
import io.vitess.client.Context;
import io.vitess.client.RpcClient;
import io.vitess.client.StreamIterator;
import io.vitess.client.VTGateBlockingConnection;
import io.vitess.client.VTSession;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.proto.Query.EventToken;
import io.vitess.proto.Query.ExecuteOptions;
import io.vitess.proto.Query.Field;
import io.vitess.proto.Query.Row;
import io.vitess.proto.Query.StreamEvent.Statement;
import io.vitess.proto.Query.StreamEvent.Statement.Category;
import io.vitess.proto.Topodata.TabletType;
import io.vitess.proto.Vtgate.UpdateStreamRequest;
import io.vitess.proto.Vtgate.UpdateStreamResponse;

/**
 * VitessUpdatesStreamExample.java is a sample for using update stream in the low-level Java client.
 * <p>
 * Before running this, start up a local example cluster as described in the
 * examples/local/README.md file.
 */
public class VitessUpdateStreamExample {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("usage: VitessUpdateStreamExample <vtgate-host:port>");
            System.exit(1);
        }

        int startingPage = ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);

        Context ctx = Context.getDefault().withDeadlineAfter(Duration.millis(5 * 1000));
        RpcClient client = new GrpcClientFactory().create(ctx, args[0]);
        VTGateBlockingConnection conn = new VTGateBlockingConnection(client);
        VTSession session = new VTSession("@master", ExecuteOptions.getDefaultInstance());

        ExampleUpdateStreamReader updateStreamReader = new ExampleUpdateStreamReader(client);
        MessagesTableWriter messagesTableWriter = new MessagesTableWriter(conn, session);

        List<UpdateStreamResponse> allResponses = new ArrayList<>();

        // Open up a connection to the update stream and read it into a queue
        BlockingQueue<UpdateStreamResponse> responseQueue = updateStreamReader.readUpdateStream();

        // We shouldn't expect anything in the update stream yet... we haven't written anything!
        UpdateStreamResponse response = responseQueue.poll(2, TimeUnit.SECONDS);
        assert response == null;

        // So let's insert a row
        System.out.println("Inserting a row. Page has id " + startingPage);
        messagesTableWriter.insertRow(startingPage);

        // And consume it from the update stream
        response = responseQueue.poll(2, TimeUnit.SECONDS);
        assert response != null;

        System.out.println("Got update from update stream: " + response);
        allResponses.add(response);

        // We can parse out the primary key column names
        // and values to get the affected row
        assert extractUpdatedRows(response).get(0).getPage() == startingPage;

        // We can continue to update rows and read the responses
        // Let's add two more entries for fun
        System.out.println("Inserting two more rows with ids " + (startingPage + 1) + " and " + (startingPage + 2));

        messagesTableWriter.insertRow(startingPage + 1);
        messagesTableWriter.insertRow(startingPage + 2);

        response = responseQueue.poll(2, TimeUnit.SECONDS);
        assert extractUpdatedRows(response).get(0).getPage() == startingPage + 1;

        System.out.println("Got update from update stream: " + response);
        allResponses.add(response);

        response = responseQueue.poll(2, TimeUnit.SECONDS);
        assert extractUpdatedRows(response).get(0).getPage() == startingPage + 2;

        System.out.println("Got update from update stream: " + response);
        allResponses.add(response);

        // Now that we know how to consume the head of the update stream,
        // let's take a look at resuming from a previous point.
        // Here we'll use the resumeTimestamp from the 2nd message we received
        // which gives us back update messages >= that timestamp
        System.out.println("Resuming update stream from middle message timestamp: " + allResponses.get(1).getResumeTimestamp());

        BlockingQueue<UpdateStreamResponse> resumedResponseQueue = updateStreamReader.readUpdateStream(
            allResponses.get(1).getResumeTimestamp());

        response = resumedResponseQueue.poll(2, TimeUnit.SECONDS);
        assert extractUpdatedRows(response).get(0).getPage() == startingPage + 1;

        System.out.println("Found second message from timestamp resume: " + response);

        response = resumedResponseQueue.poll(2, TimeUnit.SECONDS);
        assert extractUpdatedRows(response).get(0).getPage() == startingPage + 2;

        System.out.println("Found third message from timestamp resume: " + response);

        // We can also resume from an EventToken directly, rather than specifying
        // the wall-clock time of the message. We'll get everything back _after_
        // the event token. Here we'll resume from the 2nd message we saw, so we
        // only get back the 3rd message
        BlockingQueue<UpdateStreamResponse> resumedFromEventTokenResponseQueue = updateStreamReader.readUpdateStream(
            allResponses.get(1).getEvent().getEventToken());

        response = resumedFromEventTokenResponseQueue.poll(2, TimeUnit.SECONDS);
        assert extractUpdatedRows(response).get(0).getPage() == startingPage + 2;

        System.out.println("Found third message from EventToken resume: " + response);

        // We can delete all rows from the table. Since this is executed in
        // one statement, we get one update stream message w/ 3 affected rows,
        // one for each prior row we've inserted.
        messagesTableWriter.clearTable();
        response = responseQueue.poll(2, TimeUnit.SECONDS);
        assert response != null;
        assert response.getEvent().getStatementsList().size() == 1;
        assert response.getEvent().getStatementsList().get(0).getPrimaryKeyValuesCount() == 3;

        // Lastly, let's look at the parsing of the primary key's field names
        // and values. We can scroll through the field names / value lengths
        // and represent the updated row in some domain object, which may make
        // it easier to transmit to downstream services, like a cache invalidator,
        // log or separate datastore
        List<MessagesTableRow> messagesTableRows = allResponses.stream()
            .map(VitessUpdateStreamExample::extractUpdatedRows)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        for (MessagesTableRow messagesTableRow : messagesTableRows) {
            System.out.println("Domain object for message table row: " + messagesTableRow);
        }

        updateStreamReader.close();
        conn.close();
        client.close();
    }

    private static class ExampleUpdateStreamReader implements Closeable {

        private final RpcClient client;
        private volatile boolean closed;

        public ExampleUpdateStreamReader(RpcClient rpcClient) {
            this.client = rpcClient;
            this.closed = false;
        }

        private BlockingQueue<UpdateStreamResponse> readUpdateStream() throws Exception {
            return readUpdateStream(System.currentTimeMillis() / 1000);
        }

        private BlockingQueue<UpdateStreamResponse> readUpdateStream(long resumeFrom) throws Exception {
            return readUpdateStream(
                UpdateStreamRequest.newBuilder()
                    .setShard("0")
                    .setTimestamp(resumeFrom)
                    .setKeyspace("test_keyspace")
                    .setTabletType(TabletType.REPLICA)
                    .build()
            );
        }

        private BlockingQueue<UpdateStreamResponse> readUpdateStream(EventToken eventToken) throws Exception {
            return readUpdateStream(
                UpdateStreamRequest.newBuilder()
                    .setShard("0")
                    .setKeyspace("test_keyspace")
                    .setTabletType(TabletType.REPLICA)
                    .setEvent(eventToken)
                    .build()
            );
        }

        private BlockingQueue<UpdateStreamResponse> readUpdateStream(final UpdateStreamRequest initialRequest) throws Exception {
            BlockingQueue<UpdateStreamResponse> queue = Queues.newArrayBlockingQueue(10);

            Set<UpdateStreamResponse> seen = new HashSet<>();

            CompletableFuture.runAsync(() -> {
                UpdateStreamRequest updateStreamRequest = initialRequest;

                do {
                    try {
                        StreamIterator<UpdateStreamResponse> updateStream = client.getUpdateStream(
                            Context.getDefault().withDeadlineAfter(Duration.millis(5 * 1000)),
                            updateStreamRequest);

                        while (updateStream.hasNext()) {
                            UpdateStreamResponse response = updateStream.next();

                            // A real implementation would need a more sustainable
                            // way of handling duplicates when using timestamps to resume
                            if (seen.add(response)) {
                                queue.put(response);
                            }

                            updateStreamRequest = updateStreamRequest.toBuilder()
                                .clearEvent()
                                .setTimestamp(response.getResumeTimestamp())
                                .build();
                        }
                    } catch (Exception e) {
                        if (Throwables.getRootCause(e).getClass().equals(StatusRuntimeException.class)) {
                            // at the head of the stream and we didn't receive a message within
                            // the deadline timeout, so we start streaming from the head again
                        } else {
                            throw new RuntimeException(e);
                        }
                    }
                } while (!closed);
            });

            return queue;
        }

        @Override
        public void close() throws IOException {
            this.closed = true;
        }
    }

    private static class MessagesTableWriter {

        private final VTGateBlockingConnection conn;
        private final VTSession session;

        public MessagesTableWriter(VTGateBlockingConnection conn, VTSession session) {
            this.conn = conn;
            this.session = session;
        }

        public void insertRow(int page) throws Exception {
            Context ctx = Context.getDefault().withDeadlineAfter(Duration.millis(5 * 1000));

            Instant timeCreated = Instant.now();
            Map<String, Object> bindVars =
                new ImmutableMap.Builder<String, Object>()
                    .put("page", String.valueOf(page))
                    .put("time_created_ns", timeCreated.getNano())
                    .put("message", "V is for speed")
                    .build();

            conn.execute(ctx, "begin", null, session);
            conn.execute(
                ctx,
                "INSERT INTO messages (page,time_created_ns,message) VALUES (:page,:time_created_ns,:message)",
                bindVars, session);
            conn.execute(ctx, "commit", null, session);

            Thread.sleep(1000);
        }

        public void clearTable() throws Exception {
            Context ctx = Context.getDefault().withDeadlineAfter(Duration.millis(5 * 1000));

            conn.execute(ctx, "begin", null, session);
            conn.execute(
                ctx,
                "DELETE FROM messages",
                Collections.emptyMap(),
                session);
            conn.execute(ctx, "commit", null, session);
        }
    }

    private static class MessagesTableRow {
        private final long page;
        private final long timeCreatedNs;

        public MessagesTableRow(long page,
                                long timeCreatedNs) {
            this.page = page;
            this.timeCreatedNs = timeCreatedNs;
        }

        public long getPage() {
            return page;
        }

        public long getTimeCreatedNs() {
            return timeCreatedNs;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("page", page)
                .add("timeCreatedNs", timeCreatedNs)
                .toString();
        }
    }

    private static List<MessagesTableRow> extractUpdatedRows(UpdateStreamResponse updateStreamResponse) {
        List<MessagesTableRow> messagesTableUpdates = new ArrayList<>();

        for (Statement statement : updateStreamResponse.getEvent().getStatementsList()) {
            if (statement.getCategory() == Category.DDL
                || !statement.getTableName().equals("messages")) {
                continue;
            }

            List<String> primaryKeyFieldNamesInOrder = new ArrayList<>();

            for (Field field : statement.getPrimaryKeyFieldsList()) {
                primaryKeyFieldNamesInOrder.add(field.getName());
            }

            for (Row updatedRow : statement.getPrimaryKeyValuesList()) {
                int offset = 0;
                int i = 0;

                Map<String, String> fieldNameToUpdatedValue = new HashMap<>();
                for (long primaryKeyColumnValueLength : updatedRow.getLengthsList()) {
                    byte[] rawColumnValue = new byte[(int) primaryKeyColumnValueLength];
                    updatedRow.getValues().copyTo(rawColumnValue, offset, 0, (int) primaryKeyColumnValueLength);

                    fieldNameToUpdatedValue.put(
                        primaryKeyFieldNamesInOrder.get(i),
                        new String(rawColumnValue, StandardCharsets.UTF_8));

                    i++;
                    offset += primaryKeyColumnValueLength;
                }

                messagesTableUpdates.add(
                    new MessagesTableRow(
                        Long.valueOf(fieldNameToUpdatedValue.get("page")),
                        Long.valueOf(fieldNameToUpdatedValue.get("time_created_ns"))));
            }
        }

        return ImmutableList.copyOf(messagesTableUpdates);
    }

}
