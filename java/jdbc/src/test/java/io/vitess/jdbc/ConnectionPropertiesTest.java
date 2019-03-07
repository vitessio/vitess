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

import static io.vitess.util.Constants.DEFAULT_EXECUTE_TYPE;
import static io.vitess.util.Constants.DEFAULT_INCLUDED_FIELDS;
import static io.vitess.util.Constants.DEFAULT_TABLET_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import io.vitess.util.Constants;
import io.vitess.util.Constants.ZeroDateTimeBehavior;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ConnectionPropertiesTest {

  private static final int NUM_PROPS = 39;

  @Test
  public void testReflection() throws Exception {
    ConnectionProperties props = new ConnectionProperties();
    Properties info = Mockito.spy(Properties.class);
    Mockito.doReturn(info).when(info).clone();
    props.initializeProperties(info);

    // Just testing that we are properly picking up all the fields defined in the properties
    // For each field we call initializeFrom, which should call getProperty and remove
    verify(info, times(NUM_PROPS)).getProperty(anyString());
    verify(info, times(NUM_PROPS)).remove(anyString());
  }

  @Test
  public void testDefaults() throws SQLException {

    ConnectionProperties props = new ConnectionProperties();
    props.initializeProperties(new Properties());

    assertEquals("blobsAreStrings", false, props.getBlobsAreStrings());
    assertEquals("functionsNeverReturnBlobs", false, props.getFunctionsNeverReturnBlobs());
    assertEquals("tinyInt1isBit", true, props.getTinyInt1isBit());
    assertEquals("yearIsDateType", true, props.getYearIsDateType());
    assertEquals("useBlobToStoreUTF8OutsideBMP", false, props.getUseBlobToStoreUTF8OutsideBMP());
    assertEquals("utf8OutsideBmpIncludedColumnNamePattern", null,
        props.getUtf8OutsideBmpIncludedColumnNamePattern());
    assertEquals("utf8OutsideBmpExcludedColumnNamePattern", null,
        props.getUtf8OutsideBmpExcludedColumnNamePattern());
    assertEquals("zeroDateTimeBehavior", ZeroDateTimeBehavior.GARBLE,
        props.getZeroDateTimeBehavior());
    assertEquals("characterEncoding", null, props.getEncoding());
    assertEquals("executeType", DEFAULT_EXECUTE_TYPE, props.getExecuteType());
    assertEquals("twopcEnabled", false, props.getTwopcEnabled());
    assertEquals("includedFields", DEFAULT_INCLUDED_FIELDS, props.getIncludedFields());
    assertEquals("includedFieldsCache", true, props.isIncludeAllFields());
    assertEquals("tabletType", DEFAULT_TABLET_TYPE, props.getTabletType());
    assertEquals("useSSL", false, props.getUseSSL());
    assertEquals("useAffectedRows", true, props.getUseAffectedRows());
    assertEquals("refreshConnection", false, props.getRefreshConnection());
    assertEquals("refreshSeconds", 60, props.getRefreshSeconds());
  }

  @Test
  public void testInitializeFromProperties() throws SQLException {

    ConnectionProperties props = new ConnectionProperties();
    Properties info = new Properties();
    info.setProperty("blobsAreStrings", "yes");
    info.setProperty("functionsNeverReturnBlobs", "yes");
    info.setProperty("tinyInt1isBit", "yes");
    info.setProperty("yearIsDateType", "yes");
    info.setProperty("useBlobToStoreUTF8OutsideBMP", "yes");
    info.setProperty("utf8OutsideBmpIncludedColumnNamePattern", "(foo|bar)?baz");
    info.setProperty("utf8OutsideBmpExcludedColumnNamePattern", "(foo|bar)?baz");
    info.setProperty("characterEncoding", "utf-8");
    info.setProperty("zeroDateTimeBehavior", "convertToNull");
    info.setProperty("executeType", Constants.QueryExecuteType.STREAM.name());
    info.setProperty("twopcEnabled", "yes");
    info.setProperty("includedFields", Query.ExecuteOptions.IncludedFields.TYPE_ONLY.name());
    info.setProperty(Constants.Property.TABLET_TYPE, Topodata.TabletType.BACKUP.name());

    props.initializeProperties(info);

    assertEquals("blobsAreStrings", true, props.getBlobsAreStrings());
    assertEquals("functionsNeverReturnBlobs", true, props.getFunctionsNeverReturnBlobs());
    assertEquals("tinyInt1isBit", true, props.getTinyInt1isBit());
    assertEquals("yearIsDateType", true, props.getYearIsDateType());
    assertEquals("useBlobToStoreUTF8OutsideBMP", true, props.getUseBlobToStoreUTF8OutsideBMP());
    assertEquals("utf8OutsideBmpIncludedColumnNamePattern", "(foo|bar)?baz",
        props.getUtf8OutsideBmpIncludedColumnNamePattern());
    assertEquals("utf8OutsideBmpExcludedColumnNamePattern", "(foo|bar)?baz",
        props.getUtf8OutsideBmpExcludedColumnNamePattern());
    assertEquals("zeroDateTimeBehavior", ZeroDateTimeBehavior.CONVERTTONULL,
        props.getZeroDateTimeBehavior());
    assertEquals("characterEncoding", "utf-8", props.getEncoding());
    assertEquals("executeType", Constants.QueryExecuteType.STREAM, props.getExecuteType());
    assertEquals("twopcEnabled", true, props.getTwopcEnabled());
    assertEquals("includedFields", Query.ExecuteOptions.IncludedFields.TYPE_ONLY,
        props.getIncludedFields());
    assertEquals("includedFieldsCache", false, props.isIncludeAllFields());
    assertEquals("tabletType", Topodata.TabletType.BACKUP, props.getTabletType());
  }

  @Test
  public void testEncodingValidation() {
    ConnectionProperties props = new ConnectionProperties();
    Properties info = new Properties();

    String fakeEncoding = "utf-12345";
    info.setProperty("characterEncoding", fakeEncoding);
    try {
      props.initializeProperties(info);
      fail("should have failed to parse encoding " + fakeEncoding);
    } catch (SQLException e) {
      assertEquals("Unsupported character encoding: " + fakeEncoding, e.getMessage());
    }
  }

  @Test
  public void testDriverPropertiesOutput() throws SQLException {
    Properties info = new Properties();
    DriverPropertyInfo[] infos = ConnectionProperties.exposeAsDriverPropertyInfo(info, 0);
    assertEquals(NUM_PROPS, infos.length);

    // Test the expected fields for just 1
    int indexForFullTest = 3;
    assertEquals("executeType", infos[indexForFullTest].name);
    assertEquals("Query execution type: simple or stream", infos[indexForFullTest].description);
    assertEquals(false, infos[indexForFullTest].required);
    Constants.QueryExecuteType[] enumConstants = Constants.QueryExecuteType.values();
    String[] allowed = new String[enumConstants.length];
    for (int i = 0; i < enumConstants.length; i++) {
      allowed[i] = enumConstants[i].toString();
    }
    Assert.assertArrayEquals(allowed, infos[indexForFullTest].choices);

    // Test that name exists for the others, as a sanity check
    assertEquals("dbName", infos[1].name);
    assertEquals("characterEncoding", infos[2].name);
    assertEquals("executeType", infos[3].name);
    assertEquals("functionsNeverReturnBlobs", infos[4].name);
    assertEquals("grpcRetriesEnabled", infos[5].name);
    assertEquals("grpcRetriesBackoffMultiplier", infos[6].name);
    assertEquals("grpcRetriesInitialBackoffMillis", infos[7].name);
    assertEquals("grpcRetriesMaxBackoffMillis", infos[8].name);
    assertEquals(Constants.Property.INCLUDED_FIELDS, infos[9].name);
    assertEquals(Constants.Property.TABLET_TYPE, infos[21].name);
    assertEquals(Constants.Property.TWOPC_ENABLED, infos[29].name);
  }

  @Test
  public void testValidBooleanValues() throws SQLException {
    ConnectionProperties props = new ConnectionProperties();
    Properties info = new Properties();

    info.setProperty("blobsAreStrings", "true");
    info.setProperty("functionsNeverReturnBlobs", "yes");
    info.setProperty("tinyInt1isBit", "no");

    props.initializeProperties(info);

    info.setProperty(Constants.Property.TWOPC_ENABLED, "false-ish");
    try {
      props.initializeProperties(info);
      fail("should have thrown an exception on bad value false-ish");
    } catch (IllegalArgumentException e) {
      String expected = String.format(
          "Property '%s' Value 'false-ish' not in the list of allowable values: [true, false, "
              + "yes, no]",
          Constants.Property.TWOPC_ENABLED);
      assertEquals(expected, e.getMessage());
    }
  }

  @Test
  public void testValidEnumValues() throws SQLException {
    ConnectionProperties props = new ConnectionProperties();
    Properties info = new Properties();

    info.setProperty("executeType", "foo");
    try {
      props.initializeProperties(info);
      fail("should have thrown an exception on bad value foo");
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Property 'executeType' Value 'foo' not in the list of allowable values: " + Arrays
              .toString(Constants.QueryExecuteType.values()), e.getMessage());
    }
  }

  @Test
  public void testSettersUpdateCaches() throws SQLException {
    ConnectionProperties props = new ConnectionProperties();
    props.initializeProperties(new Properties());

    // included fields and all boolean cache
    assertEquals(DEFAULT_INCLUDED_FIELDS, props.getIncludedFields());
    assertTrue(props.isIncludeAllFields());

    // execute type and simple boolean cache
    assertEquals(DEFAULT_EXECUTE_TYPE, props.getExecuteType());
    assertEquals(DEFAULT_EXECUTE_TYPE == Constants.QueryExecuteType.SIMPLE,
        props.isSimpleExecute());

    // tablet type and twopc
    assertEquals(DEFAULT_TABLET_TYPE, props.getTabletType());
    assertFalse(props.getTwopcEnabled());

    props.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
    props.setExecuteType(Constants.QueryExecuteType.STREAM);
    props.setTabletType(Topodata.TabletType.BACKUP);
    props.setTwopcEnabled(true);

    // included fields and all boolean cache
    assertEquals(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME, props.getIncludedFields());
    assertFalse(props.isIncludeAllFields());

    // execute type and simple boolean cache
    assertEquals(Constants.QueryExecuteType.STREAM, props.getExecuteType());
    assertEquals(DEFAULT_EXECUTE_TYPE != Constants.QueryExecuteType.SIMPLE,
        props.isSimpleExecute());

    // tablet type and twopc
    assertEquals(Topodata.TabletType.BACKUP, props.getTabletType());
    assertTrue(props.getTwopcEnabled());
  }

  @Test
  public void testTarget() throws SQLException {
    ConnectionProperties props = new ConnectionProperties();

    // Setting keyspace
    Properties info = new Properties();
    info.setProperty(Constants.Property.KEYSPACE, "test_keyspace");
    props.initializeProperties(info);
    assertEquals("target", "test_keyspace@master", props.getTarget());

    // Setting keyspace and shard
    info = new Properties();
    info.setProperty(Constants.Property.KEYSPACE, "test_keyspace");
    info.setProperty(Constants.Property.SHARD, "80-c0");
    props.initializeProperties(info);
    assertEquals("target", "test_keyspace:80-c0@master", props.getTarget());

    // Setting tablet type
    info = new Properties();
    info.setProperty(Constants.Property.TABLET_TYPE, "replica");
    props.initializeProperties(info);
    assertEquals("target", "@replica", props.getTarget());

    // Setting shard which will have no impact without keyspace
    info = new Properties();
    info.setProperty(Constants.Property.SHARD, "80-c0");
    props.initializeProperties(info);
    assertEquals("target", "@master", props.getTarget());

    // Setting shard and tablet type. Shard will have no impact.
    info = new Properties();
    info.setProperty(Constants.Property.SHARD, "80-c0");
    info.setProperty(Constants.Property.TABLET_TYPE, "replica");
    props.initializeProperties(info);
    assertEquals("target", "@replica", props.getTarget());

    // Setting keyspace, shard and tablet type.
    info = new Properties();
    info.setProperty(Constants.Property.KEYSPACE, "test_keyspace");
    info.setProperty(Constants.Property.SHARD, "80-c0");
    info.setProperty(Constants.Property.TABLET_TYPE, "rdonly");
    props.initializeProperties(info);
    assertEquals("target", "test_keyspace:80-c0@rdonly", props.getTarget());

    // Setting keyspace, shard, tablet type and target. Target supersede others.
    info = new Properties();
    info.setProperty(Constants.Property.KEYSPACE, "test_keyspace");
    info.setProperty(Constants.Property.SHARD, "80-c0");
    info.setProperty(Constants.Property.TABLET_TYPE, "rdonly");
    info.setProperty(Constants.Property.TARGET, "dummy");
    props.initializeProperties(info);
    assertEquals("target", "dummy", props.getTarget());
  }
}
