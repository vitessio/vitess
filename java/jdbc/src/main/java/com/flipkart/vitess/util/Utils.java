package com.flipkart.vitess.util;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by harshit.gangal on 25/01/16.
 */
public class Utils {

    private Utils() {
    }

    /**
     * @param url  - Vitess Connection URL
     * @param info - Property to be extracted from URL
     * @return
     */
    public static Properties parseURLForPropertyInfo(String url, Properties info)
        throws SQLException {
        if (null == info) {
            info = new Properties();
        }

        Pattern pattern = Pattern.compile(Constants.URL_PATTERN);
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            if (null != matcher.group(2)) {
                info.setProperty(Constants.Property.HOST, matcher.group(2));
            } else {
                info.setProperty(Constants.Property.HOST, Constants.DEFAULT_HOST);
            }
            if (null != matcher.group(4)) {
                info.setProperty(Constants.Property.PORT, matcher.group(4));
            } else {
                info.setProperty(Constants.Property.PORT, Constants.DEFAULT_PORT);
            }
            if (null != matcher.group(6)) {
                info.setProperty(Constants.Property.KEYSPACE, matcher.group(6));
            } else {
                throw new SQLException(Constants.SQLExceptionMessages.KEYSPACE_REQUIRED);
            }
            if (null != matcher.group(8)) {
                info.setProperty(Constants.Property.DBNAME, matcher.group(8));
            } else {
                throw new SQLException(Constants.SQLExceptionMessages.DBNAME_REQUIRED);
            }
        }

        if (null == info.getProperty(Constants.Property.TABLET_TYPE)) {
            info.setProperty(Constants.Property.TABLET_TYPE, Constants.DEFAULT_TABLET_TYPE);
        }
        return info;
    }

    /**
     * Create the SQL string with parameters set by setXXX methods of PreparedStatement
     *
     * @param sql
     * @param parameterMap
     * @return updated SQL string
     */
    public static String getSqlWithoutParameter(String sql, Map<Integer, String> parameterMap) {
        if (!sql.contains("?")) {
            return sql;
        }

        StringBuilder newSql = new StringBuilder(sql);

        int paramLoc = 1;
        while (getCharIndexFromSqlByParamLocation(sql, '?', paramLoc) > 0) {
            // check the user has set the needs parameters
            if (parameterMap.containsKey(paramLoc)) {
                int tt = getCharIndexFromSqlByParamLocation(newSql.toString(), '?', 1);
                newSql.deleteCharAt(tt);
                newSql.insert(tt, parameterMap.get(paramLoc));
            }
            paramLoc++;
        }

        return newSql.toString();

    }

    /**
     * Get the index of given char from the SQL string by parameter location
     * </br> The -1 will be return, if nothing found
     *
     * @param sql
     * @param cchar
     * @param paramLoc
     * @return
     */
    private static int getCharIndexFromSqlByParamLocation(final String sql, final char cchar,
        final int paramLoc) {
        int signalCount = 0;
        int charIndex = -1;
        int num = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' || c == '\\')// record the count of char "'" and char "\"
            {
                signalCount++;
            } else if (c == cchar
                && signalCount % 2 == 0) {// check if the ? is really the parameter
                num++;
                if (num == paramLoc) {
                    charIndex = i;
                    break;
                }
            }
        }
        return charIndex;
    }

}
