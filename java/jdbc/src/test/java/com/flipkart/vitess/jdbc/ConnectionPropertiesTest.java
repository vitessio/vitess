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
        Mockito.verify(info, Mockito.times(4)).getProperty(Mockito.anyString());
        Mockito.verify(info, Mockito.times(4)).remove(Mockito.anyString());
    }

    @Test
    public void testDefaults() throws SQLException {

        ConnectionProperties props = new ConnectionProperties();
        props.initializeProperties(new Properties());

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
        info.setProperty("executeType", Constants.QueryExecuteType.STREAM.name());
        info.setProperty("twopcEnabled", "yes");
        info.setProperty("includedFields", Query.ExecuteOptions.IncludedFields.TYPE_ONLY.name());
        info.setProperty(Constants.Property.TABLET_TYPE, Topodata.TabletType.BACKUP.name());

        props.initializeProperties(info);

        Assert.assertEquals("executeType", Constants.QueryExecuteType.STREAM, props.getExecuteType());
        Assert.assertEquals("twopcEnabled", true, props.getTwopcEnabled());
        Assert.assertEquals("includedFields", Query.ExecuteOptions.IncludedFields.TYPE_ONLY, props.getIncludedFields());
        Assert.assertEquals("includedFieldsCache", false, props.isIncludeAllFields());
        Assert.assertEquals("tabletType", Topodata.TabletType.BACKUP, props.getTabletType());
    }

    @Test
    public void testDriverPropertiesOutput() throws SQLException {
        Properties info = new Properties();
        DriverPropertyInfo[] infos = ConnectionProperties.exposeAsDriverPropertyInfo(info, 0);
        Assert.assertEquals(4, infos.length);

        // Test the expected fields for just 1
        Assert.assertEquals("executeType", infos[0].name);
        Assert.assertEquals("Query execution type: simple or stream",
            infos[0].description);
        Assert.assertEquals(false, infos[0].required);
        Constants.QueryExecuteType[] enumConstants = Constants.QueryExecuteType.values();
        String[] allowed = new String[enumConstants.length];
        for (int i = 0; i < enumConstants.length; i++) {
            allowed[i] = enumConstants[i].toString();
        }
        Assert.assertArrayEquals(allowed, infos[0].choices);

        Assert.assertEquals(Constants.Property.TWOPC_ENABLED, infos[1].name);
        Assert.assertEquals(Constants.Property.INCLUDED_FIELDS, infos[2].name);
        Assert.assertEquals(Constants.Property.TABLET_TYPE, infos[3].name);
    }

    @Test
    public void testValidBooleanValues() throws SQLException {
        ConnectionProperties props = new ConnectionProperties();
        Properties info = new Properties();

        info.setProperty(Constants.Property.TWOPC_ENABLED, "true");
        props.initializeProperties(info);
        Assert.assertEquals(true, props.getTwopcEnabled());
        info.setProperty(Constants.Property.TWOPC_ENABLED, "yes");
        props.initializeProperties(info);
        Assert.assertEquals(true, props.getTwopcEnabled());
        info.setProperty(Constants.Property.TWOPC_ENABLED, "no");
        props.initializeProperties(info);
        Assert.assertEquals(false, props.getTwopcEnabled());

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
