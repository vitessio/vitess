package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;
import com.youtube.vitess.proto.Query;
import com.youtube.vitess.proto.Topodata;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

public class ConnectionPropertiesTest {

    @Test
    public void testReflection() throws Exception {
        ConnectionProperties props = new ConnectionProperties();
        Properties info = Mockito.spy(Properties.class);
        Mockito.doReturn(info).when(info).clone();
        props.initializeProperties(info);

        // Just testing that we are properly picking up all the fields defined in the properties
        // For each field we call initializeFrom, which should call getProperty and remove
        Mockito.verify(info, Mockito.times(13)).getProperty(Mockito.anyString());
        Mockito.verify(info, Mockito.times(13)).remove(Mockito.anyString());
    }

    @Test
    public void testDefaults() throws SQLException {

        ConnectionProperties props = new ConnectionProperties();
        props.initializeProperties(new Properties());

        Assert.assertEquals("blobsAreStrings", false, props.getBlobsAreStrings());
        Assert.assertEquals("functionsNeverReturnBlobs", false, props.getFunctionsNeverReturnBlobs());
        Assert.assertEquals("tinyInt1isBit", true, props.getTinyInt1isBit());
        Assert.assertEquals("transformedBitIsBoolean", false, props.getTransformedBitIsBoolean());
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
    }

    @Test
    public void testInitializeFromProperties() throws SQLException {

        ConnectionProperties props = new ConnectionProperties();
        Properties info = new Properties();
        info.setProperty("blobsAreStrings", "yes");
        info.setProperty("functionsNeverReturnBlobs", "yes");
        info.setProperty("tinyInt1isBit", "yes");
        info.setProperty("transformedBitIsBoolean", "yes");
        info.setProperty("yearIsDateType", "yes");
        info.setProperty("useBlobToStoreUTF8OutsideBMP", "yes");
        info.setProperty("utf8OutsideBmpIncludedColumnNamePattern", "(foo|bar)?baz");
        info.setProperty("utf8OutsideBmpExcludedColumnNamePattern", "(foo|bar)?baz");
        info.setProperty("characterEncoding", "utf-8");
        info.setProperty("clobCharacterEncoding", "utf-8");
        info.setProperty("executeType", Constants.QueryExecuteType.STREAM.name());
        info.setProperty("twopcEnabled", "yes");
        info.setProperty("includedFields", Query.ExecuteOptions.IncludedFields.TYPE_ONLY.name());
        info.setProperty(Constants.Property.TABLET_TYPE, Topodata.TabletType.BACKUP.name());

        props.initializeProperties(info);

        Assert.assertEquals("blobsAreStrings", true, props.getBlobsAreStrings());
        Assert.assertEquals("functionsNeverReturnBlobs", true, props.getFunctionsNeverReturnBlobs());
        Assert.assertEquals("tinyInt1isBit", true, props.getTinyInt1isBit());
        Assert.assertEquals("transformedBitIsBoolean", true, props.getTransformedBitIsBoolean());
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
        Assert.assertEquals(13, infos.length);

        // Test the expected fields for just 1
        Assert.assertEquals("blobsAreStrings", infos[0].name);
        Assert.assertEquals("Should the driver always treat BLOBs as Strings - specifically to work around dubious metadata returned by the server for GROUP BY clauses?",
            infos[0].description);
        Assert.assertEquals(false, infos[0].required);
        Assert.assertArrayEquals(new String[]{Boolean.toString(true), Boolean.toString(false), "yes", "no"}, infos[0].choices);

        // Otherwise just test that they exist in there
        Assert.assertEquals("functionsNeverReturnBlobs", infos[1].name);
        Assert.assertEquals("tinyInt1isBit", infos[2].name);
        Assert.assertEquals("transformedBitIsBoolean", infos[3].name);
        Assert.assertEquals("yearIsDateType", infos[4].name);
        Assert.assertEquals("useBlobToStoreUTF8OutsideBMP", infos[5].name);
        Assert.assertEquals("utf8OutsideBmpIncludedColumnNamePattern", infos[6].name);
        Assert.assertEquals("utf8OutsideBmpExcludedColumnNamePattern", infos[7].name);
        Assert.assertEquals("characterEncoding", infos[8].name);
        Assert.assertEquals(Constants.Property.EXECUTE_TYPE, infos[9].name);
        Assert.assertEquals(Constants.Property.TWOPC_ENABLED, infos[10].name);
        Assert.assertEquals(Constants.Property.INCLUDED_FIELDS, infos[11].name);
        Assert.assertEquals(Constants.Property.TABLET_TYPE, infos[12].name);
    }

    @Test
    public void testValidBooleanValues() throws SQLException {
        ConnectionProperties props = new ConnectionProperties();
        Properties info = new Properties();

        info.setProperty("blobsAreStrings", "true");
        info.setProperty("functionsNeverReturnBlobs", "yes");
        info.setProperty("tinyInt1isBit", "no");

        props.initializeProperties(info);

        Assert.assertEquals(true, props.getBlobsAreStrings());
        Assert.assertEquals(true, props.getFunctionsNeverReturnBlobs());
        Assert.assertEquals(false, props.getTinyInt1isBit());

        info.setProperty("blobsAreStrings", "false-ish");
        try {
            props.initializeProperties(info);
            Assert.fail("should have thrown an exception on bad value false-ish");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(
                "Property 'blobsAreStrings' Value 'false-ish' not in the list of allowable values: "
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

        // execute type and simple boolean cahce
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

        // execute type and simple boolean cahce
        Assert.assertEquals(Constants.QueryExecuteType.STREAM, props.getExecuteType());
        Assert.assertEquals(Constants.DEFAULT_EXECUTE_TYPE != Constants.QueryExecuteType.SIMPLE, props.isSimpleExecute());

        // tablet type and twopc
        Assert.assertEquals(Topodata.TabletType.BACKUP, props.getTabletType());
        Assert.assertEquals(true, props.getTwopcEnabled());
    }
}
