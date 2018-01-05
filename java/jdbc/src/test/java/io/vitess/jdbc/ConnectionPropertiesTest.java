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

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import io.vitess.util.Constants;

public class ConnectionPropertiesTest {

    private static final int NUM_PROPS = 36;

    @Test
    public void testReflection() throws Exception {
        ConnectionProperties props = new ConnectionProperties();
        Properties info = Mockito.spy(Properties.class);
        Mockito.doReturn(info).when(info).clone();
        props.initializeProperties(info);

        // Just testing that we are properly picking up all the fields defined in the properties
        // For each field we call initializeFrom, which should call getProperty and remove
        Mockito.verify(info, Mockito.times(NUM_PROPS)).getProperty(Mockito.anyString());
        Mockito.verify(info, Mockito.times(NUM_PROPS)).remove(Mockito.anyString());
    }

    @Test
    public void testDefaults() throws SQLException {

        ConnectionProperties props = new ConnectionProperties();
        props.initializeProperties(new Properties());

        Assert.assertEquals("blobsAreStrings", false, props.getBlobsAreStrings());
        Assert.assertEquals("functionsNeverReturnBlobs", false, props.getFunctionsNeverReturnBlobs());
        Assert.assertEquals("tinyInt1isBit", true, props.getTinyInt1isBit());
        Assert.assertEquals("yearIsDateType", true, props.getYearIsDateType());
        Assert.assertEquals("useBlobToStoreUTF8OutsideBMP", false, props.getUseBlobToStoreUTF8OutsideBMP());
        Assert.assertEquals("utf8OutsideBmpIncludedColumnNamePattern", null, props.getUtf8OutsideBmpIncludedColumnNamePattern());
        Assert.assertEquals("utf8OutsideBmpExcludedColumnNamePattern", null, props.getUtf8OutsideBmpExcludedColumnNamePattern());
        Assert.assertEquals("characterEncoding", null, props.getEncoding());
        Assert.assertEquals("executeType", Constants.DEFAULT_EXECUTE_TYPE, props.getExecuteType());
        Assert.assertEquals("twopcEnabled", false, props.getTwopcEnabled());
        Assert.assertEquals("includedFields", Constants.DEFAULT_INCLUDED_FIELDS, props.getIncludedFields());
        Assert.assertEquals("includedFieldsCache", true, props.isIncludeAllFields());
        Assert.assertEquals("tabletType", Constants.DEFAULT_TABLET_TYPE, props.getTabletType());
        Assert.assertEquals("useSSL", false, props.getUseSSL());
        Assert.assertEquals("useAffectedRows", true, props.getUseAffectedRows());
        Assert.assertEquals("refreshConnection", false, props.getRefreshConnection());
        Assert.assertEquals("refreshSeconds", 60, props.getRefreshSeconds());
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
        info.setProperty("executeType", Constants.QueryExecuteType.STREAM.name());
        info.setProperty("twopcEnabled", "yes");
        info.setProperty("includedFields", Query.ExecuteOptions.IncludedFields.TYPE_ONLY.name());
        info.setProperty(Constants.Property.TABLET_TYPE, Topodata.TabletType.BACKUP.name());

        props.initializeProperties(info);

        Assert.assertEquals("blobsAreStrings", true, props.getBlobsAreStrings());
        Assert.assertEquals("functionsNeverReturnBlobs", true, props.getFunctionsNeverReturnBlobs());
        Assert.assertEquals("tinyInt1isBit", true, props.getTinyInt1isBit());
        Assert.assertEquals("yearIsDateType", true, props.getYearIsDateType());
        Assert.assertEquals("useBlobToStoreUTF8OutsideBMP", true, props.getUseBlobToStoreUTF8OutsideBMP());
        Assert.assertEquals("utf8OutsideBmpIncludedColumnNamePattern", "(foo|bar)?baz", props.getUtf8OutsideBmpIncludedColumnNamePattern());
        Assert.assertEquals("utf8OutsideBmpExcludedColumnNamePattern", "(foo|bar)?baz", props.getUtf8OutsideBmpExcludedColumnNamePattern());
        Assert.assertEquals("characterEncoding", "utf-8", props.getEncoding());
        Assert.assertEquals("executeType", Constants.QueryExecuteType.STREAM, props.getExecuteType());
        Assert.assertEquals("twopcEnabled", true, props.getTwopcEnabled());
        Assert.assertEquals("includedFields", Query.ExecuteOptions.IncludedFields.TYPE_ONLY, props.getIncludedFields());
        Assert.assertEquals("includedFieldsCache", false, props.isIncludeAllFields());
        Assert.assertEquals("tabletType", Topodata.TabletType.BACKUP, props.getTabletType());
    }

    @Test(expected = SQLException.class)
    public void testEncodingValidation() throws SQLException {
        ConnectionProperties props = new ConnectionProperties();
        Properties info = new Properties();

        String fakeEncoding = "utf-12345";
        info.setProperty("characterEncoding", fakeEncoding);
        try {
            props.initializeProperties(info);
            Assert.fail("should have failed to parse encoding " + fakeEncoding);
        } catch (SQLException e) {
            Assert.assertEquals("Unsupported character encoding: " + fakeEncoding, e.getMessage());
            throw e;
        }
    }

    @Test
    public void testDriverPropertiesOutput() throws SQLException {
        Properties info = new Properties();
        DriverPropertyInfo[] infos = ConnectionProperties.exposeAsDriverPropertyInfo(info, 0);
        Assert.assertEquals(NUM_PROPS, infos.length);

        // Test the expected fields for just 1
        int indexForFullTest = 15;
        Assert.assertEquals("executeType", infos[indexForFullTest].name);
        Assert.assertEquals("Query execution type: simple or stream",
            infos[indexForFullTest].description);
        Assert.assertEquals(false, infos[indexForFullTest].required);
        Constants.QueryExecuteType[] enumConstants = Constants.QueryExecuteType.values();
        String[] allowed = new String[enumConstants.length];
        for (int i = 0; i < enumConstants.length; i++) {
            allowed[i] = enumConstants[i].toString();
        }
        Assert.assertArrayEquals(allowed, infos[indexForFullTest].choices);

        // Test that name exists for the others, as a sanity check
        Assert.assertEquals("functionsNeverReturnBlobs", infos[1].name);
        Assert.assertEquals("tinyInt1isBit", infos[2].name);
        Assert.assertEquals("yearIsDateType", infos[3].name);
        Assert.assertEquals("useBlobToStoreUTF8OutsideBMP", infos[4].name);
        Assert.assertEquals("utf8OutsideBmpIncludedColumnNamePattern", infos[5].name);
        Assert.assertEquals("utf8OutsideBmpExcludedColumnNamePattern", infos[6].name);
        Assert.assertEquals("characterEncoding", infos[7].name);
        Assert.assertEquals(Constants.Property.TWOPC_ENABLED, infos[indexForFullTest+1].name);
        Assert.assertEquals(Constants.Property.INCLUDED_FIELDS, infos[indexForFullTest+2].name);
        Assert.assertEquals(Constants.Property.TABLET_TYPE, infos[indexForFullTest-2].name);
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
            Assert.fail("should have thrown an exception on bad value false-ish");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                "Property '" + Constants.Property.TWOPC_ENABLED + "' Value 'false-ish' not in the list of allowable values: "
                    + Arrays.toString(new String[] { Boolean.toString(true), Boolean.toString(false), "yes", "no"})
                , e.getMessage());
        }

    }

    @Test
    public void testValidEnumValues() throws SQLException {
        ConnectionProperties props = new ConnectionProperties();
        Properties info = new Properties();

        info.setProperty("executeType", "foo");
        try {
            props.initializeProperties(info);
            Assert.fail("should have thrown an exception on bad value foo");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                "Property 'executeType' Value 'foo' not in the list of allowable values: "
                    + Arrays.toString(Constants.QueryExecuteType.values())
                , e.getMessage());
        }
    }

    @Test
    public void testSettersUpdateCaches() throws SQLException {
        ConnectionProperties props = new ConnectionProperties();
        props.initializeProperties(new Properties());

        // included fields and all boolean cache
        Assert.assertEquals(Constants.DEFAULT_INCLUDED_FIELDS, props.getIncludedFields());
        Assert.assertEquals(true, props.isIncludeAllFields());

        // execute type and simple boolean cache
        Assert.assertEquals(Constants.DEFAULT_EXECUTE_TYPE, props.getExecuteType());
        Assert.assertEquals(Constants.DEFAULT_EXECUTE_TYPE == Constants.QueryExecuteType.SIMPLE, props.isSimpleExecute());

        // tablet type and twopc
        Assert.assertEquals(Constants.DEFAULT_TABLET_TYPE, props.getTabletType());
        Assert.assertEquals(false, props.getTwopcEnabled());

        props.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        props.setExecuteType(Constants.QueryExecuteType.STREAM);
        props.setTabletType(Topodata.TabletType.BACKUP);
        props.setTwopcEnabled(true);

        // included fields and all boolean cache
        Assert.assertEquals(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME, props.getIncludedFields());
        Assert.assertEquals(false, props.isIncludeAllFields());

        // execute type and simple boolean cache
        Assert.assertEquals(Constants.QueryExecuteType.STREAM, props.getExecuteType());
        Assert.assertEquals(Constants.DEFAULT_EXECUTE_TYPE != Constants.QueryExecuteType.SIMPLE, props.isSimpleExecute());

        // tablet type and twopc
        Assert.assertEquals(Topodata.TabletType.BACKUP, props.getTabletType());
        Assert.assertEquals(true, props.getTwopcEnabled());
    }

    @Test
    public void testTarget() throws SQLException {
        ConnectionProperties props = new ConnectionProperties();

        // Setting keyspace
        Properties info = new Properties();
        info.setProperty(Constants.Property.KEYSPACE, "test_keyspace");
        props.initializeProperties(info);
        Assert.assertEquals("target", "test_keyspace@master", props.getTarget());

        // Setting keyspace and shard
        info = new Properties();
        info.setProperty(Constants.Property.KEYSPACE, "test_keyspace");
        info.setProperty(Constants.Property.SHARD, "80-c0");
        props.initializeProperties(info);
        Assert.assertEquals("target", "test_keyspace:80-c0@master", props.getTarget());

        // Setting tablet type
        info = new Properties();
        info.setProperty(Constants.Property.TABLET_TYPE, "replica");
        props.initializeProperties(info);
        Assert.assertEquals("target", "@replica", props.getTarget());

        // Setting shard which will have no impact without keyspace
        info = new Properties();
        info.setProperty(Constants.Property.SHARD, "80-c0");
        props.initializeProperties(info);
        Assert.assertEquals("target", "@master", props.getTarget());

        // Setting shard and tablet type. Shard will have no impact.
        info = new Properties();
        info.setProperty(Constants.Property.SHARD, "80-c0");
        info.setProperty(Constants.Property.TABLET_TYPE, "replica");
        props.initializeProperties(info);
        Assert.assertEquals("target", "@replica", props.getTarget());

        // Setting keyspace, shard and tablet type.
        info = new Properties();
        info.setProperty(Constants.Property.KEYSPACE, "test_keyspace");
        info.setProperty(Constants.Property.SHARD, "80-c0");
        info.setProperty(Constants.Property.TABLET_TYPE, "rdonly");
        props.initializeProperties(info);
        Assert.assertEquals("target", "test_keyspace:80-c0@rdonly", props.getTarget());

        // Setting keyspace, shard, tablet type and target. Target supersede others.
        info = new Properties();
        info.setProperty(Constants.Property.KEYSPACE, "test_keyspace");
        info.setProperty(Constants.Property.SHARD, "80-c0");
        info.setProperty(Constants.Property.TABLET_TYPE, "rdonly");
        info.setProperty(Constants.Property.TARGET, "dummy");
        props.initializeProperties(info);
        Assert.assertEquals("target", "dummy", props.getTarget());
    }
}
