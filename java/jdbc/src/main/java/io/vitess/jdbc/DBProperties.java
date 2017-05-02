package io.vitess.jdbc;

/**
 * Created by ashudeep.sharma on 10/03/16.
 */
public class DBProperties {
    private final String productversion;
    private final String majorVersion;
    private final String minorVersion;
    private final int isolationLevel;
    private final boolean caseInsensitiveComparison;
    private final boolean storesLowerCaseTableName;

    public DBProperties(String productversion, String majorVersion, String minorVersion,
                        int isolationLevel, String lowerCaseTableNames) {

        this.productversion = productversion;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.isolationLevel = isolationLevel;
        this.caseInsensitiveComparison = "1".equalsIgnoreCase(lowerCaseTableNames) || "2".equalsIgnoreCase(lowerCaseTableNames);
        this.storesLowerCaseTableName = "1".equalsIgnoreCase(lowerCaseTableNames);
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

    public boolean getUseCaseInsensitiveComparisons() {
        return caseInsensitiveComparison;
    }

    public boolean getStoresLowerCaseTableName() {
        return storesLowerCaseTableName;
    }
}
