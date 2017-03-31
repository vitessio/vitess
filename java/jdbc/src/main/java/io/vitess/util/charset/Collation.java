package io.vitess.util.charset;

/**
 * These classes were pulled from mysql-connector-java and simplified to just the parts supporting the statically available
 * charsets
 */
class Collation {
    public final int index;
    public final String collationName;
    public final int priority;
    public final MysqlCharset mysqlCharset;

    public Collation(int index, String collationName, int priority, String charsetName) {
        this.index = index;
        this.collationName = collationName;
        this.priority = priority;
        this.mysqlCharset = CharsetMapping.CHARSET_NAME_TO_CHARSET.get(charsetName);
    }

    @Override
    public String toString() {
        return "[" +
            "index=" +
            this.index +
            ",collationName=" +
            this.collationName +
            ",charsetName=" +
            this.mysqlCharset.charsetName +
            ",javaCharsetName=" +
            this.mysqlCharset.getMatchingJavaEncoding(null) +
            "]";
    }
}
