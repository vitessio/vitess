package com.flipkart.vitess.util;

import java.util.Map;

/**
 * Created by naveen.nahata on 05/02/16.
 */
public class StringUtils {

    private StringUtils() {
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string
     * 'searchFor', disregarding case and leading whitespace
     *
     * @param searchIn  the string to search in
     * @param searchFor the string to search for
     * @return true if the string starts with 'searchFor' ignoring whitespace
     */
    public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor) {
        return startsWithIgnoreCaseAndWs(searchIn, searchFor, 0);
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string
     * 'searchFor', disregarding case and leading whitespace
     *
     * @param searchIn  the string to search in
     * @param searchFor the string to search for
     * @param beginPos  where to start searching
     * @return true if the string starts with 'searchFor' ignoring whitespace
     */

    public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor,
        int beginPos) {
        if (null == searchIn) {
            return true;
        }

        int inLength = searchIn.length();

        for (; beginPos < inLength; beginPos++) {
            if (!Character.isWhitespace(searchIn.charAt(beginPos))) {
                break;
            }
        }

        return startsWithIgnoreCase(searchIn, beginPos, searchFor);
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string
     * 'searchFor', dis-regarding case starting at 'startAt' Shorthand for a
     * String.regionMatch(...)
     *
     * @param searchIn  the string to search in
     * @param startAt   the position to start at
     * @param searchFor the string to search for
     * @return whether searchIn starts with searchFor, ignoring case
     */
    public static boolean startsWithIgnoreCase(String searchIn, int startAt, String searchFor) {
        return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor.length());
    }

    public static boolean isNullOrEmptyWithoutWS(String string) {
        return null == string || 0 == string.trim().length();
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

    /**
     * Adds '+' to decimal numbers that are positive (MySQL doesn't understand
     * them otherwise
     *
     * @param decimalString The value as a string
     * @return String the string with a '+' added (if needed)
     */
    public static String fixDecimalExponent(String decimalString) {
        int ePos = decimalString.indexOf('E');

        if (ePos == -1) {
            ePos = decimalString.indexOf('e');
        }

        if (ePos != -1) {
            if (decimalString.length() > (ePos + 1)) {
                char maybeMinusChar = decimalString.charAt(ePos + 1);

                if (maybeMinusChar != '-' && maybeMinusChar != '+') {
                    StringBuilder strBuilder = new StringBuilder(decimalString.length() + 1);
                    strBuilder.append(decimalString.substring(0, ePos + 1));
                    strBuilder.append('+');
                    strBuilder.append(decimalString.substring(ePos + 1, decimalString.length()));
                    decimalString = strBuilder.toString();
                }
            }
        }

        return decimalString;
    }
}
