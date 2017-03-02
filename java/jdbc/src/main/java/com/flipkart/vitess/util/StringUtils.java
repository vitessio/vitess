package com.flipkart.vitess.util;

import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by naveen.nahata on 05/02/16.
 */
public class StringUtils {

    private static final String platformEncoding = System.getProperty("file.encoding");
    private static final ConcurrentHashMap<String, Charset> charsetsByAlias = new ConcurrentHashMap<String, Charset>();

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

    /*
     * DateTime Format Parsing Logic from Mysql JDBC
     */
    public static String getDateTimePattern(String dt, boolean toTime) throws Exception {
        int dtLength = (dt != null) ? dt.length() : 0;

        if ((dtLength >= 8) && (dtLength <= 10)) {
            int dashCount = 0;
            boolean isDateOnly = true;

            for (int i = 0; i < dtLength; i++) {
                char c = dt.charAt(i);

                if (!Character.isDigit(c) && (c != '-')) {
                    isDateOnly = false;

                    break;
                }

                if (c == '-') {
                    dashCount++;
                }
            }

            if (isDateOnly && (dashCount == 2)) {
                return "yyyy-MM-dd";
            }
        }

        // Special case - time-only
        boolean colonsOnly = true;
        for (int i = 0; i < dtLength; i++) {
            char c = dt.charAt(i);

            if (!Character.isDigit(c) && (c != ':')) {
                colonsOnly = false;

                break;
            }
        }

        if (colonsOnly) {
            return "HH:mm:ss";
        }

        int n;
        int z;
        int count;
        int maxvecs;
        char c;
        char separator;
        StringReader reader = new StringReader(dt + " ");
        ArrayList<Object[]> vec = new ArrayList<>();
        ArrayList<Object[]> vecRemovelist = new ArrayList<>();
        Object[] nv = new Object[3];
        Object[] v;
        nv[0] = 'y';
        nv[1] = new StringBuilder();
        nv[2] = 0;
        vec.add(nv);

        if (toTime) {
            nv = new Object[3];
            nv[0] = 'h';
            nv[1] = new StringBuilder();
            nv[2] = 0;
            vec.add(nv);
        }

        while ((z = reader.read()) != -1) {
            separator = (char) z;
            maxvecs = vec.size();

            for (count = 0; count < maxvecs; count++) {
                v = vec.get(count);
                n = (Integer) v[2];
                c = getSuccessor((Character) v[0], n);

                if (!Character.isLetterOrDigit(separator)) {
                    if ((c == (Character) v[0]) && (c != 'S')) {
                        vecRemovelist.add(v);
                    } else {
                        ((StringBuilder) v[1]).append(separator);

                        if ((c == 'X') || (c == 'Y')) {
                            v[2] = 4;
                        }
                    }
                } else {
                    if (c == 'X') {
                        c = 'y';
                        nv = new Object[3];
                        nv[1] = (new StringBuilder((v[1]).toString())).append('M');
                        nv[0] = 'M';
                        nv[2] = 1;
                        vec.add(nv);
                    } else if (c == 'Y') {
                        c = 'M';
                        nv = new Object[3];
                        nv[1] = (new StringBuilder((v[1]).toString())).append('d');
                        nv[0] = 'd';
                        nv[2] = 1;
                        vec.add(nv);
                    }

                    ((StringBuilder) v[1]).append(c);
                    if (c == (Character) v[0]) {
                        v[2] = n + 1;
                    } else {
                        v[0] = c;
                        v[2] = 1;
                    }
                }
            }

            for (Object[] aVecRemovelist : vecRemovelist) {
                v = aVecRemovelist;
                vec.remove(v);
            }
            vecRemovelist.clear();
        }

        int size = vec.size();
        for (int i = 0; i < size; i++) {
            v = vec.get(i);
            c = (Character) v[0];
            n = (Integer) v[2];

            boolean bk = getSuccessor(c, n) != c;
            boolean atEnd = (((c == 's') || (c == 'm') || ((c == 'h') && toTime)) && bk);
            boolean finishesAtDate = (bk && (c == 'd') && !toTime);
            boolean containsEnd = ((v[1]).toString().indexOf('W') != -1);

            if ((!atEnd && !finishesAtDate) || (containsEnd)) {
                vecRemovelist.add(v);
            }
        }

        size = vecRemovelist.size();

        for (int i = 0; i < size; i++) {
            vec.remove(vecRemovelist.get(i));
        }

        vecRemovelist.clear();
        v = vec.get(0); // might throw exception

        StringBuilder format = (StringBuilder) v[1];
        format.setLength(format.length() - 1);

        return format.toString();
    }

    private static char getSuccessor(char c, int n) {
        return ((c == 'y') && (n == 2)) ?
            'X' :
            (((c == 'y') && (n < 4)) ?
                'y' :
                ((c == 'y') ?
                    'M' :
                    (((c == 'M') && (n == 2)) ?
                        'Y' :
                        (((c == 'M') && (n < 3)) ?
                            'M' :
                            ((c == 'M') ?
                                'd' :
                                (((c == 'd') && (n < 2)) ?
                                    'd' :
                                    ((c == 'd') ?
                                        'H' :
                                        (((c == 'H') && (n < 2)) ?
                                            'H' :
                                            ((c == 'H') ?
                                                'm' :
                                                (((c == 'm') && (n < 2)) ?
                                                    'm' :
                                                    ((c == 'm') ?
                                                        's' :
                                                        (((c == 's') && (n < 2)) ?
                                                            's' :
                                                            'W'))))))))))));
    }

    /**
     * Finds the true start of a SQL statement, by skipping leading comments.
     * If the query is multiple lines
     * @param sql to parse
     * @return position index in string
     */
    public static int findStartOfStatement(String sql) {
        int statementStartPos = 0;
        if (StringUtils.startsWithIgnoreCaseAndWs(sql, "/*")) {
            statementStartPos = sql.indexOf("*/");

            if (statementStartPos == -1) {
                statementStartPos = 0;
            } else {
                statementStartPos += 2;
            }
        } else if (StringUtils.startsWithIgnoreCaseAndWs(sql, "--") || StringUtils.startsWithIgnoreCaseAndWs(sql, "#")) {
            statementStartPos = sql.indexOf('\n');

            if (statementStartPos == -1) {
                statementStartPos = sql.indexOf('\r');

                if (statementStartPos == -1) {
                    statementStartPos = 0;
                }
            }
        }
        return statementStartPos;
    }

    public static String toString(byte[] value, int offset, int length, String encoding) throws UnsupportedEncodingException {
        Charset cs = findCharset(encoding);
        return cs.decode(ByteBuffer.wrap(value, offset, length)).toString();
    }

    public static String toString(byte[] value, String encoding) throws UnsupportedEncodingException {
        return findCharset(encoding)
            .decode(ByteBuffer.wrap(value))
            .toString();
    }

    public static String toString(byte[] value, int offset, int length) {
        try {
            return findCharset(platformEncoding)
                .decode(ByteBuffer.wrap(value, offset, length))
                .toString();
        } catch (UnsupportedEncodingException e) {
            // can't happen, emulating new String(byte[])
        }
        return null;
    }

    public static String toString(byte[] value) {
        try {
            return findCharset(platformEncoding)
                .decode(ByteBuffer.wrap(value))
                .toString();
        } catch (UnsupportedEncodingException e) {
            // can't happen, emulating new String(byte[])
        }
        return null;
    }

    public static byte[] getBytes(String value, String encoding) throws UnsupportedEncodingException {
        return getBytes(value, 0, value.length(), encoding);
    }

    public static byte[] getBytes(String value, int offset, int length, String encoding) throws UnsupportedEncodingException {
        Charset cs = findCharset(encoding);
        ByteBuffer buf = cs.encode(CharBuffer.wrap(value.toCharArray(), offset, length));
        // can't simply .array() this to get the bytes especially with variable-length charsets the buffer is sometimes larger than the actual encoded data
        int encodedLen = buf.limit();
        byte[] asBytes = new byte[encodedLen];
        buf.get(asBytes, 0, encodedLen);
        return asBytes;
    }

    private static Charset findCharset(String alias) throws UnsupportedEncodingException {
        try {
            Charset cs = charsetsByAlias.get(alias);
            if (cs == null) {
                cs = Charset.forName(alias);
                Charset oldCs = charsetsByAlias.putIfAbsent(alias, cs);
                if (oldCs != null) {
                    // if the previous value was recently set by another thread we return it instead of value we found here
                    cs = oldCs;
                }
            }
            return cs;
            // We re-throw these runtimes for compatibility with java.io
        } catch (IllegalArgumentException iae) {
            throw new UnsupportedEncodingException(alias);
        }
    }
}
