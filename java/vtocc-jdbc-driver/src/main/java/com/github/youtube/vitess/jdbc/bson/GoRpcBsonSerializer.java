package com.github.youtube.vitess.jdbc.bson;

import com.google.common.base.Joiner;
import com.google.common.primitives.Bytes;
import com.google.inject.Inject;
import com.google.protobuf.Message;

import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;
import org.bson.ExtendableBsonDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class for serialisation of {@link com.github.youtube.vitess.jdbc.vtocc.QueryService} protobuf
 * messages to BSON bytes.
 */
public class GoRpcBsonSerializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      GoRpcBsonSerializer.class);


  private final SqlQueryBsonRequestBodyFactory sqlQueryBsonRequestBodyFactory;
  private final SqlQueryResponseFactory sqlQueryResponseFactory;
  private final PythonicBsonEncoder pythonicBsonEncoder;
  private final GoRpcBsonDecoder goRpcBsonDecoder;

  @Inject
  public GoRpcBsonSerializer(SqlQueryBsonRequestBodyFactory sqlQueryBsonRequestBodyFactory,
      SqlQueryResponseFactory sqlQueryResponseFactory, PythonicBsonEncoder pythonicBsonEncoder,
      GoRpcBsonDecoder goRpcBsonDecoder) {
    this.sqlQueryBsonRequestBodyFactory = sqlQueryBsonRequestBodyFactory;
    this.sqlQueryResponseFactory = sqlQueryResponseFactory;
    this.pythonicBsonEncoder = pythonicBsonEncoder;
    this.goRpcBsonDecoder = goRpcBsonDecoder;
  }

  /**
   * Encodes a {@link com.github.youtube.vitess.jdbc.vtocc.QueryService.SqlQuery} request and {@code
   * methodName} into bson.
   */
  public byte[] encode(String methodName, int sequence, Message request) {
    BSONObject bsonHeader = createBsonHeader(methodName, sequence);
    BSONObject bsonBody = sqlQueryBsonRequestBodyFactory.create(request);
    return Bytes.concat(
        pythonicBsonEncoder.encode(bsonHeader), pythonicBsonEncoder.encode(bsonBody));
  }

  /**
   * Decodes bson byte header and checks for error.
   */
  @SuppressWarnings("UnusedParameters")
  public void checkHeader(String methodName, InputStream inputStream) throws IOException {
    // TODO(gco): Check sequence matches and method matches.
    BSONObject bsonHeader = goRpcBsonDecoder.readObject(inputStream);
    if (LOGGER.isDebugEnabled()) {
      String joinedBsonHeader = Joiner.on(',').withKeyValueSeparator("=").join(bsonHeader.toMap());
      LOGGER.debug("bsonHeader: {}", joinedBsonHeader);
    }
    if (bsonHeader.containsField("Error")) {
      Object error = bsonHeader.get("Error");
      if (error instanceof byte[] && ((byte[]) error).length != 0) {
        throw new IOException(new String((byte[]) error, StandardCharsets.UTF_8));
      }
    }
  }

  /**
   * Decodes bson byte body to {@link com.github.youtube.vitess.jdbc.vtocc.QueryService.SqlQuery}
   * response.
   */
  public Message decodeBody(String methodName, InputStream inputStream) throws IOException {
    BSONObject bsonBody = goRpcBsonDecoder.readObject(inputStream);
    return sqlQueryResponseFactory.create(methodName, bsonBody);
  }


  protected BSONObject createBsonHeader(String methodName, int sequence) {
    BSONObject bsonObject = new BasicBSONObject();
    bsonObject.put("ServiceMethod", methodToString(methodName));
    bsonObject.put("Seq", sequence);
    return bsonObject;
  }

  /**
   * Converts the method to the string needed by ServiceMethod.
   */
  protected String methodToString(String methodName) {
    return "SqlQuery." + methodName;
  }

  /**
   * Modified to match python bson encoder.
   */
  protected static class PythonicBsonEncoder extends BasicBSONEncoder {

    /**
     * Outputs binary because python bson doesn't use strings.
     */
    @Override
    protected void putString(String name, String s) {
      putBinary(name, s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Converts long to int where possible to match python bson.
     */
    @Override
    protected void putNumber(String name, Number n) {
      if (n instanceof Long) {
        if (n.longValue() < Integer.MAX_VALUE && n.longValue() > Integer.MIN_VALUE) {
          super.putNumber(name, n.intValue());
          return;
        }
      }
      super.putNumber(name, n);
    }

  }

  /**
   * Modified to match GoRpc bson encoder.
   *
   * @see #decodeElement()
   */
  public static class GoRpcBsonDecoder extends ExtendableBsonDecoder {

    /**
     * {@code 0x3f} is a valid type not supported by {@link org.bson.BasicBSONDecoder} but {@link
     * org.bson.BasicBSONDecoder#decodeElement()} is package private and deep inside.
     *
     * We catch the error and handle it rather than duplicating the implementation.
     */
    @SuppressWarnings("deprecation")
    @Override
    protected boolean decodeElement() throws IOException {
      try {
        return super.decodeElement();
      } catch (UnsupportedOperationException e) {
        Matcher matcher = Pattern.compile(
            "\\btype\\s*:\\s*([^\\s]+).*\\bname\\s*:\\s*([^\\s]+)").matcher(e.getMessage());
        if (matcher.find()) {
          // 0x3f is 63
          if ("63".equals(matcher.group(1))) {
            _callback.gotLong(matcher.group(2), _in.readLong());
            return true;
          }
        }
        throw e;
      }
    }
  }
}
