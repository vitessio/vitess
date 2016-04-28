package com.flipkart.vitess.jdbc;

/**
 * Created by ashudeep.sharma on 10/03/16.
 */
public class DBProperties {
    private String productversion;
    private String majorVersion;
    private String minorVersion;
    private int isolationLevel;

    public DBProperties(String productversion, String majorVersion, String minorVersion,
        int isolationLevel) {

        this.productversion = productversion;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.isolationLevel = isolationLevel;
    }

    public String getProductversion() {
        return this.productversion;
    }

    public String getMajorVersion() {
        return this.majorVersion;
    }

    public String getMinorVersion() {
        return this.minorVersion;
    }

    public int getIsolationLevel() {
        return this.isolationLevel;
    }
}
