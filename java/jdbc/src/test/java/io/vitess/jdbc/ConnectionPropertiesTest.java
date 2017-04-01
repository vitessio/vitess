package io.vitess.jdbc;

import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import io.vitess.util.Constants;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ConnectionPropertiesTest {

    private static final int NUM_PROPS = 20;

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
        int indexForFullTest = 8;
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
        Assert.assertEquals(Constants.Property.TWOPC_ENABLED, infos[9].name);
        Assert.assertEquals(Constants.Property.INCLUDED_FIELDS, infos[10].name);
        Assert.assertEquals(Constants.Property.TABLET_TYPE, infos[11].name);
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
