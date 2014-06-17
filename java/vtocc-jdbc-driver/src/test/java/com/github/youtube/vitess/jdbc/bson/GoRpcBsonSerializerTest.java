package com.github.youtube.vitess.jdbc.bson;

import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import com.github.youtube.vitess.jdbc.vtocc.QueryService.BindVariable;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.BindVariable.Type;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.BoundQuery;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.Cell;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.Field;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.Query;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.QueryList;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.QueryResult;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.Row;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.Session;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.SessionParams;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Test class for creation of Bson serialisation to be sent on wire to server.
 *
 * <p>In each case the bson serialisation of an RPC is compared to bson encoding from {@code
 * queryservice_test.py}.
 *
 * @see #unixStringToBytes(String)
 */
@RunWith(JUnit4.class)
public class GoRpcBsonSerializerTest {

  private GoRpcBsonSerializer goRpcBsonSerializer;

  /**
   * Converts from the a unix string copied from terminal from my vim to bytes.
   *
   * <p>Implementation is only good enough for tests.
   *
   * <p>Special characters converted are of the form {@code ^A},{@code <97>}.
   */
  public static byte[] unixStringToBytes(String unixString) throws IOException {
    ByteArrayOutputStream res = new ByteArrayOutputStream();
    for (int i = 0; i < unixString.length(); i++) {
      char c = unixString.charAt(i);
      if (c == '^') {
        i++;
        c = unixString.charAt(i);

        Preconditions.checkArgument(c >= 0x40 && c <= 0x71);
        res.write(c - 0x40);
      } else if (c == '<') {
        res.write(Integer.parseInt(unixString.substring(i + 1, i + 3), 16));
        i += 3;
      } else {
        res.write(c);
      }
    }
    return res.toByteArray();
  }

  @Before
  public void setup() {
    goRpcBsonSerializer = Guice.createInjector().getInstance(GoRpcBsonSerializer.class);
  }

  @Test
  public void testBsonEncodeExecute() throws Exception {
    Message message = Query.newBuilder()
        .setSession(Session.newBuilder()
            .setSessionId(6450935864881530835L)
            .setTransactionId(1402598754830561039L)
            .build())
        .setSql(ByteString.copyFrom("insert into vtocc_test values(4, null, null, \'\\0"
                + "\\\'\\\"\\b\\n\\r\\t\\Z\\\\\u0000\u000f\u00f0\u00ff\')",
            StandardCharsets.ISO_8859_1.name()))
        .build();

    String expectedUnixBson = "2^@^@^@^EServiceMethod^@^P^@^@^@^@SqlQuery.Execute^PSeq^@^D^@^@^@^@"
        + "<93>^@^@^@^RTransactionId^@^O¯^Tà\"^Hw^S^RSessionId^@ÓG^W<97>ýS<86>Y^CBindVariables^@^E"
        + "^@^@^@^@^ESql^@F^@^@^@^@insert into vtocc_test values(4, null, null, '\\0\\'\\\"\\b\\n"
        + "\\r\\t\\Z\\\\^@^Oðÿ')^@";
    byte[] expectedBytesBson = unixStringToBytes(expectedUnixBson);
    int sequence = 4;

    Assert.assertArrayEquals(expectedBytesBson,
        goRpcBsonSerializer.encode("Execute", sequence, message));
  }

  @Test
  public void testBsonEncodeGetSessionId() throws Exception {

    Message message = SessionParams.newBuilder()
        .setKeyspace("test_keyspace")
        .setShard("0")
        .build();

    String expectedUnixBson = "7^@^@^@^EServiceMethod^@^U^@^@^@^@SqlQuery.GetSessionId^PSeq^@^A^@"
        + "^@^@^@.^@^@^@^EKeyspace^@^M^@^@^@^@test_keyspace^EShard^@^A^@^@^@^@0^@";
    byte[] expectedBytesBson = unixStringToBytes(expectedUnixBson);
    int sequence = 1;

    Assert.assertArrayEquals(expectedBytesBson,
        goRpcBsonSerializer.encode("GetSessionId", sequence, message));
  }

  @Test
  public void testBsonEncodeExecuteBatch() throws Exception {
    Message message = QueryList.newBuilder()
        .setSession(Session.newBuilder()
            .setSessionId(6450935864881530835L)
            .setTransactionId(0)
            .build())
        .addQueries(BoundQuery.newBuilder()
            .addBindVariables(BindVariable.newBuilder()
                .setName("a")
                .setType(Type.valueOf(2))
                .setValueInt(2)
                .build())
            .setSql(ByteString.copyFrom("select * from vtocc_a where id = :a",
                StandardCharsets.ISO_8859_1.name()))
            .build())
        .addQueries(BoundQuery.newBuilder()
            .addBindVariables(BindVariable.newBuilder()
                .setName("b")
                .setType(Type.valueOf(2))
                .setValueInt(2)
                .build())
            .setSql(ByteString.copyFrom("select * from vtocc_b where id = :b",
                StandardCharsets.ISO_8859_1.name()))
            .build())
        .build();

    String expectedUnixBson = "7^@^@^@^EServiceMethod^@^U^@^@^@^@SqlQuery."
        + "ExecuteBatch^PSeq^@^B^@^@^@^@Ù^@^@^@^PTransactionId^@^@^@^@^@^R"
        + "SessionId^@ÓG^W<97>ýS<86>Y^DQueries^@¥^@^@^@^C0^@M^@^@^@^CBindVariables"
        + "^@^L^@^@^@^Pa^@^B^@^@^@^@^ESql^@#^@^@^@^@select * from vtocc_a where id "
        + "= :a^@^C1^@M^@^@^@^CBindVariables^@^L^@^@^@^Pb^@^B^@^@^@^@^ESql^@#^@^@^@^@select * "
        + "from vtocc_b where id = :b^@^@^@";
    byte[] expectedBytesBson = unixStringToBytes(expectedUnixBson);
    int sequence = 2;

    Assert.assertArrayEquals(expectedBytesBson,
        goRpcBsonSerializer.encode("ExecuteBatch", sequence, message));
  }

  @Test
  public void testBsonEncodeBegin() throws Exception {
    Message message = Session.newBuilder()
        .setSessionId(6450935864881530835L)
        .setTransactionId(0)
        .build();

    String expectedUnixBson = "0^@^@^@^EServiceMethod^@^N^@^@^@^@SqlQuery.Begin^PSeq^@^C^@^@^@^@+"
        + "^@^@^@^PTransactionId^@^@^@^@^@^RSessionId^@ÓG^W<97>ýS<86>Y^@";
    byte[] expectedBytesBson = unixStringToBytes(expectedUnixBson);
    int sequence = 3;

    Assert.assertArrayEquals(expectedBytesBson,
        goRpcBsonSerializer.encode("Begin", sequence, message));
  }

  @Test
  public void testBsonDecodeQuery() throws Exception {
    Message expectedResponse = QueryResult.newBuilder()
        .setInsertId(0)
        .addFields(Field.newBuilder()
            .setName("1")
            .setType(Field.Type.valueOf(8))
            .build())
        .addRows(Row.newBuilder()
            .addValues(Cell.newBuilder().setValue(ByteString.copyFromUtf8("1"))))
        .addRows(Row.newBuilder()
            .addValues(Cell.newBuilder().setValue(ByteString.copyFromUtf8("1"))))
        .addRows(Row.newBuilder()
            .addValues(Cell.newBuilder().setValue(ByteString.copyFromUtf8("1"))))
        .setRowsAffected(3L)
        .build();

    String inputUnixBson = "<9a>^@^@^@^DFields^@'^@^@^@^C0^@^_^@^@^@^EName^@^A^@^@^@^@1^R"
        + "Type^@^H^@^@^@^@^@^@^@^@^@?RowsAffected^@^C^@^@^@^@^@^@^@?InsertId"
        + "^@^@^@^@^@^@^@^@^@^DRows^@8^@^@^@^D0^@^N^@^@^@^E0^@^A^@^@^@^@1^@^D"
        + "1^@^N^@^@^@^E0^@^A^@^@^@^@1^@^D2^@^N^@^@^@^E0^@^A^@^@^@^@1^@^@^@";

    byte[] inputBytesBson = unixStringToBytes(inputUnixBson);

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(inputBytesBson);

    Assert.assertEquals(expectedResponse,
        goRpcBsonSerializer.decodeBody("Execute", byteArrayInputStream));
  }

}