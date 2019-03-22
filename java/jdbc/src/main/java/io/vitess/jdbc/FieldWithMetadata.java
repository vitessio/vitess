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

package io.vitess.jdbc;

import com.google.common.annotations.VisibleForTesting;

import io.vitess.proto.Query;
import io.vitess.util.Constants;
import io.vitess.util.MysqlDefs;
import io.vitess.util.StringUtils;
import io.vitess.util.charset.CharsetMapping;

import java.sql.SQLException;
import java.sql.Types;
import java.util.regex.PatternSyntaxException;

public class FieldWithMetadata {

  private final ConnectionProperties connectionProperties;
  private final Query.Field field;
  private final Query.Type vitessType;
  private final boolean isImplicitTempTable;
  private final boolean isSingleBit;
  private final int precisionAdjustFactor;

  private int javaType;
  private int colFlag;
  private String encoding;
  private String collationName;
  private int collationIndex;
  private int maxBytesPerChar;

  public FieldWithMetadata(ConnectionProperties connectionProperties, Query.Field field)
      throws SQLException {
    this.connectionProperties = connectionProperties;
    this.field = field;
    this.colFlag = field.getFlags();
    this.vitessType = field.getType();
    this.collationIndex = field.getCharset();

    // Map MySqlTypes to an initial java.sql Type
    // Afterwards, below we will sometimes re-map the javaType based on other
    // information we receive from the server, such as flags and encodings.
    if (MysqlDefs.vitesstoMySqlType.containsKey(vitessType)) {
      this.javaType = MysqlDefs.vitesstoMySqlType.get(vitessType);
    } else if (field.getType().equals(Query.Type.TUPLE)) {
      throw new SQLException(Constants.SQLExceptionMessages.INVALID_COLUMN_TYPE);
    } else {
      throw new SQLException(Constants.SQLExceptionMessages.UNKNOWN_COLUMN_TYPE);
    }

    // All of the below remapping and metadata fields require the extra
    // fields included when includeFields=IncludedFields.ALL
    if (connectionProperties != null && connectionProperties.isIncludeAllFields()) {
      this.isImplicitTempTable = checkForImplicitTemporaryTable();
      // Re-map  BLOB to 'real' blob type
      if (this.javaType == Types.BLOB) {
        boolean isFromFunction = field.getOrgTable().isEmpty();
        if (connectionProperties.getBlobsAreStrings() || (
            connectionProperties.getFunctionsNeverReturnBlobs() && isFromFunction)) {
          this.javaType = Types.VARCHAR;
        } else if (collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary) {
          if (connectionProperties.getUseBlobToStoreUTF8OutsideBMP()
              && shouldSetupForUtf8StringInBlob()) {
            if (this.getColumnLength() == MysqlDefs.LENGTH_TINYBLOB
                || this.getColumnLength() == MysqlDefs.LENGTH_BLOB) {
              this.javaType = Types.VARCHAR;
            } else {
              this.javaType = Types.LONGVARCHAR;
            }
            this.collationIndex = CharsetMapping.MYSQL_COLLATION_INDEX_utf8;
          } else {
            if (this.getColumnLength() == MysqlDefs.LENGTH_TINYBLOB) {
              this.javaType = Types.VARBINARY;
            } else if (this.getColumnLength() == MysqlDefs.LENGTH_BLOB
                || this.getColumnLength() == MysqlDefs.LENGTH_MEDIUMBLOB
                || this.getColumnLength() == MysqlDefs.LENGTH_LONGBLOB) {
              this.javaType = Types.LONGVARBINARY;
            }
          }
        } else {
          // *TEXT masquerading as blob
          this.javaType = Types.LONGVARCHAR;
        }
      }

      // Re-map TINYINT(1) as bit or pseudo-boolean
      if (this.javaType == Types.TINYINT && this.field.getColumnLength() == 1
          && connectionProperties.getTinyInt1isBit()) {
        this.javaType = Types.BIT;
      }

      if (!isNativeNumericType() && !isNativeDateTimeType()) {
        // For non-numeric types, try to pull the encoding from the passed collationIndex
        // We will do some fixup afterwards
        this.encoding = getEncodingForIndex(this.collationIndex);
        // ucs2, utf16, and utf32 cannot be used as a client character set, but if it was
        // received from server
        // under some circumstances we can parse them as utf16
        if ("UnicodeBig".equals(this.encoding)) {
          this.encoding = "UTF-16";
        }
        // MySQL always encodes JSON data with utf8mb4. Discard whatever else we've found, if the
        // type is JSON
        if (vitessType == Query.Type.JSON) {
          this.encoding = "UTF-8";
        }
        this.isSingleBit = this.javaType == Types.BIT && (field.getColumnLength() == 0
            || field.getColumnLength() == 1);

        // The server sends back a BINARY/VARBINARY field whenever varchar/text data is stored on
        // disk as binary, but
        // that doesn't mean the data is actually binary. For instance, a field with collation
        // ascii_bin
        // gets stored on disk as bytes for case-sensitive comparison, but is still an ascii string.
        // Re-map these BINARY/VARBINARY types to CHAR/VARCHAR when the data is not actually
        // binary encoded
        boolean isBinaryEncoded =
            isBinary() && collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary;
        if (javaType == Types.VARBINARY && !isBinaryEncoded) {
          this.javaType = Types.VARCHAR;
        }
        if (javaType == Types.BINARY && !isBinaryEncoded) {
          this.javaType = Types.CHAR;
        }
      } else {
        // Default encoding for number-types and date-types
        // We keep the default javaType as passed from the server, and just set the encoding
        this.encoding = "US-ASCII";
        this.isSingleBit = false;
      }

      // Precision can be calculated from column length, but needs
      // to be adjusted for the extra bytes used by the negative sign
      // and decimal points, where appropriate.
      if (isSigned()) {
        switch (javaType) {
          case Types.FLOAT:
          case Types.REAL:
          case Types.DOUBLE:
          case Types.BIT:
            // float/real/double are the same regardless of sign/decimal
            // bit values can't actually be signed
            this.precisionAdjustFactor = 0;
            break;
          default:
            // other types we adjust for the negative symbol, and decimal
            // symbol if there are decimals
            this.precisionAdjustFactor = getDecimals() > 0 ? -2 : -1;
        }
      } else {
        switch (javaType) {
          case Types.DECIMAL:
          case Types.NUMERIC:
            // adjust for the decimal
            this.precisionAdjustFactor = -1;
            break;
          default:
            // all other types need no adjustment
            this.precisionAdjustFactor = 0;
        }
      }
    } else {
      // MySQL always encodes JSON data with utf8mb4. Discard whatever else we've found, if the
      // type is JSON
      if (vitessType == Query.Type.JSON) {
        this.encoding = "UTF-8";
      }
      // Defaults to appease final variables when not including all fields
      this.isImplicitTempTable = false;
      this.isSingleBit = false;
      this.precisionAdjustFactor = 0;
    }
  }

  /**
   * Implicit temp tables are temporary tables created internally by MySQL for certain operations.
   * For those types of tables, the table name is always prefixed with #sql_, typically followed by
   * a numeric or other unique identifier.
   */
  private boolean checkForImplicitTemporaryTable() {
    return field.getTable().length() > 5 && field.getTable().startsWith("#sql_");
  }

  private boolean isNativeNumericType() {
    switch (this.javaType) {
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.FLOAT:
      case Types.DOUBLE:
      case Types.REAL:
      case Types.DECIMAL:
        return true;
      default:
        return false;
    }
  }

  private boolean isNativeDateTimeType() {
    switch (this.javaType) {
      case Types.DATE:
      case Types.TIME:
      case Types.TIMESTAMP:
        return true;
      default:
        return false;
    }
  }

  @VisibleForTesting
  String getEncodingForIndex(int charsetIndex) {
    String javaEncoding = null;
    if (charsetIndex != MysqlDefs.NO_CHARSET_INFO) {
      javaEncoding = CharsetMapping
          .getJavaEncodingForCollationIndex(charsetIndex, connectionProperties.getEncoding());
    }
    // If nothing, get default based on configuration, may still be null
    if (javaEncoding == null) {
      javaEncoding = connectionProperties.getEncoding();
    }
    return javaEncoding;
  }

  public ConnectionProperties getConnectionProperties() throws SQLException {
    checkConnectionProperties();
    return connectionProperties;
  }

  public boolean hasConnectionProperties() {
    return connectionProperties != null;
  }

  private void checkConnectionProperties() throws SQLException {
    if (!hasConnectionProperties()) {
      throw new SQLException(Constants.SQLExceptionMessages.CONN_UNAVAILABLE);
    }
  }

  private boolean shouldSetupForUtf8StringInBlob() throws SQLException {
    String includePattern = connectionProperties.getUtf8OutsideBmpIncludedColumnNamePattern();
    String excludePattern = connectionProperties.getUtf8OutsideBmpExcludedColumnNamePattern();

    // When UseBlobToStoreUTF8OutsideBMP is set, we by default set blobs to UTF-8. So we first
    // look for fields to exclude from that remapping (blacklist)
    if (excludePattern != null && !StringUtils.isNullOrEmptyWithoutWS(excludePattern)) {
      try {
        if (getOrgName().matches(excludePattern)) {
          // If we want to include more specific patters that were inadvertently covered by the
          // exclude pattern,
          // we set the includePattern (whitelist)
          if (includePattern != null && !StringUtils.isNullOrEmptyWithoutWS(includePattern)) {
            try {
              if (getOrgName().matches(includePattern)) {
                return true;
              }
            } catch (PatternSyntaxException pse) {
              throw new SQLException(
                  "Illegal regex specified for \"utf8OutsideBmpIncludedColumnNamePattern\"", pse);
            }
          }
          return false;
        }
      } catch (PatternSyntaxException pse) {
        throw new SQLException(
            "Illegal regex specified for \"utf8OutsideBmpExcludedColumnNamePattern\"", pse);
      }
    }

    return true;
  }

  public boolean isAutoIncrement() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return false;
    }
    return ((this.colFlag & Query.MySqlFlag.AUTO_INCREMENT_FLAG_VALUE) > 0);
  }

  public boolean isBinary() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return false;
    }
    return ((this.colFlag & Query.MySqlFlag.BINARY_FLAG_VALUE) > 0);
  }

  public boolean isBlob() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return false;
    }
    return ((this.colFlag & Query.MySqlFlag.BLOB_FLAG_VALUE) > 0);
  }

  public boolean isMultipleKey() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return false;
    }
    return ((this.colFlag & Query.MySqlFlag.MULTIPLE_KEY_FLAG_VALUE) > 0);
  }

  boolean isNotNull() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return true;
    }
    return ((this.colFlag & Query.MySqlFlag.NOT_NULL_FLAG_VALUE) > 0);
  }

  public boolean isZeroFill() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return false;
    }
    return ((this.colFlag & Query.MySqlFlag.ZEROFILL_FLAG_VALUE) > 0);
  }

  public boolean isPrimaryKey() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return false;
    }
    return ((this.colFlag & Query.MySqlFlag.PRI_KEY_FLAG_VALUE) > 0);
  }

  public boolean isUniqueKey() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return false;
    }
    return ((this.colFlag & Query.MySqlFlag.UNIQUE_KEY_FLAG_VALUE) > 0);
  }

  public boolean isUnsigned() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return true;
    }
    return ((this.colFlag & Query.MySqlFlag.UNSIGNED_FLAG_VALUE) > 0);
  }

  public boolean isSigned() throws SQLException {
    checkConnectionProperties();
    return !isUnsigned();
  }

  boolean isOpaqueBinary() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return false;
    }

    // Detect CHAR(n) CHARACTER SET BINARY which is a synonym for fixed-length binary types
    if (this.collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary && isBinary() && (
        this.vitessType == Query.Type.CHAR || this.vitessType == Query.Type.VARCHAR)) {
      // Okay, queries resolved by temp tables also have this 'signature', check for that
      return !isImplicitTemporaryTable();
    }

    // this is basically always false unless a valid charset is not found and someone explicitly
    // sets a fallback
    // using ConnectionProperties, as binary defaults to ISO8859-1 per mysql-connector-j
    // implementation
    return "binary".equalsIgnoreCase(getEncoding());
  }

  /**
   * Is this field _definitely_ not writable?
   *
   * @return true if this field can not be written to in an INSERT/UPDATE statement.
   */
  boolean isReadOnly() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return false;
    }
    String orgColumnName = getOrgName();
    String orgTableName = getOrgTable();
    return !(orgColumnName != null && orgColumnName.length() > 0 && orgTableName != null
        && orgTableName.length() > 0);
  }

  public synchronized String getCollation() throws SQLException {
    if (!connectionProperties.isIncludeAllFields()) {
      return null;
    }

    if (this.collationName == null) {
      int collationIndex = getCollationIndex();
      try {
        this.collationName = CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[collationIndex];
      } catch (ArrayIndexOutOfBoundsException ex) {
        throw new SQLException("CollationIndex '" + collationIndex
            + "' out of bounds for collationName lookup, should be within 0 and "
            + CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME.length, ex);
      }
    }
    return this.collationName;
  }


  public synchronized int getMaxBytesPerCharacter() {
    if (!connectionProperties.isIncludeAllFields()) {
      return 0;
    }

    if (this.maxBytesPerChar == 0) {
      this.maxBytesPerChar = getMaxBytesPerChar(getCollationIndex(), getEncoding());
    }
    return this.maxBytesPerChar;
  }

  @VisibleForTesting
  int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName) {
    // if we can get it by charsetIndex just doing it
    String charset = CharsetMapping.getMysqlCharsetNameForCollationIndex(charsetIndex);
    // if we didn't find charset name by its full name
    if (charset == null) {
      charset = CharsetMapping.getMysqlCharsetForJavaEncoding(javaCharsetName);
    }
    // checking against static maps
    return CharsetMapping.getMblen(charset);
  }

  public String getName() {
    return field.getName();
  }

  public String getTable() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return null;
    }
    return field.getTable();
  }

  public String getOrgTable() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return null;
    }
    return field.getOrgTable();
  }

  public String getDatabase() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return null;
    }
    return field.getDatabase();
  }

  public String getOrgName() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return null;
    }
    return field.getOrgName();
  }

  public int getColumnLength() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return 0;
    }
    return field.getColumnLength();
  }

  public int getDecimals() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return 0;
    }
    return field.getDecimals();
  }

  public int getJavaType() {
    return javaType;
  }

  private Query.Type getVitessType() {
    return vitessType;
  }

  public int getVitessTypeValue() {
    return field.getTypeValue();
  }

  boolean isImplicitTemporaryTable() {
    if (!connectionProperties.isIncludeAllFields()) {
      return false;
    }
    return isImplicitTempTable;
  }

  public String getEncoding() {
    if (!connectionProperties.isIncludeAllFields()) {
      return null;
    }
    return encoding;
  }

  /**
   * Precision can be calculated from column length, but needs to be adjusted for the extra values
   * that can be included for the various numeric types
   */
  public int getPrecisionAdjustFactor() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return 0;
    }
    return precisionAdjustFactor;
  }

  public boolean isSingleBit() throws SQLException {
    checkConnectionProperties();
    if (!connectionProperties.isIncludeAllFields()) {
      return false;
    }
    return isSingleBit;
  }

  private int getCollationIndex() {
    return collationIndex;
  }

  @Override
  public String toString() {
    try {
      StringBuilder asString = new StringBuilder();
      asString.append(getClass().getCanonicalName());
      asString.append("[");
      asString.append("catalog=");
      asString.append(this.getDatabase());
      asString.append(",tableName=");
      asString.append(this.getTable());
      asString.append(",originalTableName=");
      asString.append(this.getOrgTable());
      asString.append(",columnName=");
      asString.append(this.getName());
      asString.append(",originalColumnName=");
      asString.append(this.getOrgName());
      asString.append(",vitessType=");
      asString.append(getVitessType());
      asString.append("(");
      asString.append(getJavaType());
      asString.append(")");
      asString.append(",flags=");
      if (isAutoIncrement()) {
        asString.append("AUTO_INCREMENT");
      }
      if (isPrimaryKey()) {
        asString.append(" PRIMARY_KEY");
      }
      if (isUniqueKey()) {
        asString.append(" UNIQUE_KEY");
      }
      if (isBinary()) {
        asString.append(" BINARY");
      }
      if (isBlob()) {
        asString.append(" BLOB");
      }
      if (isMultipleKey()) {
        asString.append(" MULTI_KEY");
      }
      if (isUnsigned()) {
        asString.append(" UNSIGNED");
      }
      if (isZeroFill()) {
        asString.append(" ZEROFILL");
      }

      asString.append(", charsetIndex=");
      asString.append(this.collationIndex);
      asString.append(", charsetName=");
      asString.append(this.encoding);
      asString.append("]");
      return asString.toString();
    } catch (Throwable ignored) {
      return super.toString();
    }
  }
}
