package com.github.youtube.vitess.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.github.youtube.vitess.jdbc.QueryService.BindVariable;
import com.github.youtube.vitess.jdbc.QueryService.BindVariable.Type;
import com.github.youtube.vitess.jdbc.QueryService.Query;

import acolyte.StatementHandler.Parameter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.List;

import javax.inject.Inject;

/**
 * Builds {@link Query} from SQL and Acolyte-provided {@link Parameter}s.
 *
 * Serializes Java types into MySQL/Vtocc supported strings.
 *
 * Instances are only called from within {@link acolyte.Driver}, contract is not well defined
 * therefore there are no unit tests. This code is tested as a part of integration tests
 * running SQL queries through JDBC.
 */
public class VtoccQueryFactory {

  private final VtoccTransactionHandler vtoccTransactionHandler;

  @Inject
  @VisibleForTesting
  VtoccQueryFactory(VtoccTransactionHandler vtoccTransactionHandler) {
    this.vtoccTransactionHandler = vtoccTransactionHandler;
  }

  public Query create(String sql, List<Parameter> parameters) throws SQLException {
    Query.Builder builder = Query.newBuilder();
    builder.setSession(vtoccTransactionHandler.getSession());

    // TODO(timofeyb): remove regexp when ; at the end of the statement is supported in Vtocc
    builder.setSql(ByteString.copyFromUtf8(sql.replaceAll(";\\s*$", "")));
    int i = 1;
    for (Parameter parameter : parameters) {
      Object value = parameter.getValue();
      if (value == null) {
        builder.addBindVariables(BindVariable.newBuilder()
            .setName(getParamName(i))
            .setType(Type.NULL)
            .build());
      } else if (byte[].class.getName().equals(parameter.getLeft().className)) {
        Preconditions.checkArgument(value instanceof byte[],
              "Parameter type and value do not match: "
                  + parameter.getLeft() + " / " + value.getClass().getName());
        builder.addBindVariables(BindVariable.newBuilder()
            .setName(getParamName(i))
            .setType(Type.BYTES)
            .setValueBytes(ByteString.copyFrom((byte[]) value))
            .build());
      } else if (String.class.getName().equals(parameter.getLeft().className)) {
        Preconditions.checkArgument(value instanceof String,
            "Parameter type and value do not match: "
                + parameter.getLeft() + " / " + value.getClass().getName());
        builder.addBindVariables(BindVariable.newBuilder()
            .setName(getParamName(i))
            .setType(Type.BYTES)
            .setValueBytes(ByteString.copyFromUtf8((String) value))
            .build());
      } else if (Boolean.class.getName().equals(parameter.getLeft().className)) {
        Preconditions.checkArgument(value instanceof Boolean,
            "Parameter type and value do not match: "
                + parameter.getLeft() + " / " + value.getClass().getName());
        builder.addBindVariables(BindVariable.newBuilder()
            .setName(getParamName(i))
            .setType(Type.INT)
            .setValueInt((Boolean) value ? 1 : 0)
            .build());
      } else if (Byte.class.getName().equals(parameter.getLeft().className)) {
        Preconditions.checkArgument(value instanceof Byte,
            "Parameter type and value do not match: "
                + parameter.getLeft() + " / " + value.getClass().getName());
        builder.addBindVariables(BindVariable.newBuilder()
            .setName(getParamName(i))
            .setType(Type.INT)
            .setValueInt((Byte) value)
            .build());
      } else if (Short.class.getName().equals(parameter.getLeft().className)) {
        Preconditions.checkArgument(value instanceof Short,
            "Parameter type and value do not match: "
                + parameter.getLeft() + " / " + value.getClass().getName());
        builder.addBindVariables(BindVariable.newBuilder()
            .setName(getParamName(i))
            .setType(Type.INT)
            .setValueInt((Short) value)
            .build());
      } else if (Integer.class.getName().equals(parameter.getLeft().className)) {
        Preconditions.checkArgument(value instanceof Integer,
            "Parameter type and value do not match: "
                + parameter.getLeft() + " / " + value.getClass().getName());
        builder.addBindVariables(BindVariable.newBuilder()
            .setName(getParamName(i))
            .setType(Type.INT)
            .setValueInt((Integer) value)
            .build());
      } else if (Long.class.getName().equals(parameter.getLeft().className)) {
        Preconditions.checkArgument(value instanceof Long,
            "Parameter type and value do not match: "
                + parameter.getLeft() + " / " + value.getClass().getName());
        builder.addBindVariables(BindVariable.newBuilder()
            .setName(getParamName(i))
            .setType(Type.INT)
            .setValueInt((Long) value)
            .build());
      } else if (Float.class.getName().equals(parameter.getLeft().className)) {
        Preconditions.checkArgument(value instanceof Float,
            "Parameter type and value do not match: "
                + parameter.getLeft() + " / " + value.getClass().getName());
        builder.addBindVariables(BindVariable.newBuilder()
            .setName(getParamName(i))
            .setType(Type.FLOAT)
            .setValueFloat((Float) value)
            .build());
      } else if (Double.class.getName().equals(parameter.getLeft().className)) {
        Preconditions.checkArgument(value instanceof Double,
            "Parameter type and value do not match: "
                + parameter.getLeft() + " / " + value.getClass().getName());
        builder.addBindVariables(BindVariable.newBuilder()
            .setName(getParamName(i))
            .setType(Type.FLOAT)
            .setValueFloat((Double) value)
            .build());
      } else if (BigInteger.class.getName().equals(parameter.getLeft().className)) {
        Preconditions.checkArgument(value instanceof BigInteger,
            "Parameter type and value do not match: "
                + parameter.getLeft() + " / " + value.getClass().getName());
        builder.addBindVariables(BindVariable.newBuilder()
            .setName(getParamName(i))
            .setType(Type.BYTES)
            .setValueBytes(ByteString.copyFromUtf8(value.toString()))
            .build());
      } else if (BigDecimal.class.getName().equals(parameter.getLeft().className)) {
        Preconditions.checkArgument(value instanceof BigDecimal,
            "Parameter type and value do not match: "
                + parameter.getLeft() + " / " + value.getClass().getName());
        builder.addBindVariables(BindVariable.newBuilder()
            .setName(getParamName(i))
            .setType(Type.BYTES)
            .setValueBytes(ByteString.copyFromUtf8(value.toString()))
            .build());
      } else {
        throw new IllegalArgumentException("Unknown parameter type: " + parameter);
      }
      i++;
    }
    return builder.build();
  }

  /**
   * Returns magical parameter name supported by Vtocc.
   */
  private static String getParamName(int i) {
    return "v" + Integer.toString(i);
  }
}
