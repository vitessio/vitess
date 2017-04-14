package io.vitess.util;

import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by naveen.nahata on 05/02/16.
 */
public class StringUtils {

    private static final String platformEncoding = System.getProperty("file.encoding");
    private static final ConcurrentHashMap<String, Charset> charsetsByAlias = new ConcurrentHashMap<String, Charset>();

    // length of MySQL version reference in comments of type '/*![00000] */'
    private static final int NON_COMMENTS_MYSQL_VERSION_REF_LENGTH = 5;


    private StringUtils() {
    }

    public enum SearchMode {
        ALLOW_BACKSLASH_ESCAPE, SKIP_BETWEEN_MARKERS, SKIP_BLOCK_COMMENTS, SKIP_LINE_COMMENTS, SKIP_WHITE_SPACE;
    }

    /*
     * Convenience EnumSets for several SearchMode combinations
     */

    /**
     * Full search mode: allow backslash escape, skip between markers, skip block comments, skip line comments and skip white space.
     */
    public static final Set<SearchMode> SEARCH_MODE__ALL = Collections.unmodifiableSet(EnumSet.allOf(SearchMode.class));

    /**
     * Search mode: skip between markers, skip block comments, skip line comments and skip white space.
     */
    public static final Set<SearchMode> SEARCH_MODE__MRK_COM_WS = Collections.unmodifiableSet(
      EnumSet.of(SearchMode.SKIP_BETWEEN_MARKERS, SearchMode.SKIP_BLOCK_COMMENTS, SearchMode.SKIP_LINE_COMMENTS, SearchMode.SKIP_WHITE_SPACE));

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

    private static boolean isCharEqualIgnoreCase(char charToCompare, char compareToCharUC, char compareToCharLC) {
        return Character.toLowerCase(charToCompare) == compareToCharLC || Character.toUpperCase(charToCompare) == compareToCharUC;
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

    /**
     * Searches for a quoteChar in the searchIn string
     * @param searchIn
     * @param quoteChar
     * @param startFrom
     * @return
     */
    public static int indexOfQuoteDoubleAware(String searchIn, String quoteChar, int startFrom) {
        if (searchIn == null || quoteChar == null || quoteChar.length() == 0 || startFrom > searchIn.length()) {
            return -1;
        }
        int lastIndex = searchIn.length() - 1;
        int beginPos = startFrom;
        int pos = -1;
        boolean next = true;
        while (next) {
            pos = searchIn.indexOf(quoteChar, beginPos);
            if (pos == -1 || pos == lastIndex || !searchIn.startsWith(quoteChar, pos + 1)) {
                next = false;
            } else {
                beginPos = pos + 2;
            }
        }
        return pos;
    }

    /**
     * Quotes an identifier, escaping any dangling quotes within
     * @param identifier
     * @param quoteChar
     * @return
     */
    public static String quoteIdentifier(String identifier, String quoteChar) {
        if (identifier == null) {
            return null;
        }
        identifier = identifier.trim();
        int quoteCharLength = quoteChar.length();
        if (quoteCharLength == 0 || " ".equals(quoteChar)) {
            return identifier;
        }

        if (identifier.startsWith(quoteChar) && identifier.endsWith(quoteChar)) {
            String identifierQuoteTrimmed = identifier.substring(quoteCharLength, identifier.length() - quoteCharLength);
            int quoteCharPos = identifierQuoteTrimmed.indexOf(quoteChar);
            while (quoteCharPos >= 0) {
                int quoteCharNextExpectedPos = quoteCharPos + quoteCharLength;
                int quoteCharNextPosition = identifierQuoteTrimmed.indexOf(quoteChar, quoteCharNextExpectedPos);

                if (quoteCharNextPosition == quoteCharNextExpectedPos) {
                    quoteCharPos = identifierQuoteTrimmed.indexOf(quoteChar, quoteCharNextPosition + quoteCharLength);
                } else {
                    // Not a pair of quotes!
                    break;
                }
            }

            if (quoteCharPos < 0) {
                return identifier;
            }
        }

        return quoteChar + identifier.replaceAll(quoteChar, quoteChar + quoteChar) + quoteChar;
    }

    /**
     * Convenience function for {@link #indexOfIgnoreCase(int, String, String, String, String, Set)}, passing {@link #SEARCH_MODE__ALL}
     */
    public static int indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor, String openingMarkers, String closingMarkers) {
        return indexOfIgnoreCase(startingPosition, searchIn, searchFor, openingMarkers, closingMarkers, SEARCH_MODE__ALL);
    }

    /**
     * Finds the position of a substring within a string, ignoring case, with the option to skip text delimited by given markers or within comments.
     *
     * @param startingPosition
     *            the position to start the search from
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     * @param openingMarkers
     *            characters which delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters which delimit the end of a text block to skip
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behavior of the search
     * @return the position where <code>searchFor</code> is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor, String openingMarkers, String closingMarkers,
                                        Set<SearchMode> searchMode) {
        if (searchIn == null || searchFor == null) {
            return -1;
        }

        int searchInLength = searchIn.length();
        int searchForLength = searchFor.length();
        int stopSearchingAt = searchInLength - searchForLength;

        if (startingPosition > stopSearchingAt || searchForLength == 0) {
            return -1;
        }

        if (searchMode.contains(SearchMode.SKIP_BETWEEN_MARKERS)
          && (openingMarkers == null || closingMarkers == null || openingMarkers.length() != closingMarkers.length())) {
            throw new IllegalArgumentException("Must specify a valid openingMarkers and closingMarkers");
        }

        // Some locales don't follow upper-case rule, so need to check both
        char firstCharOfSearchForUc = Character.toUpperCase(searchFor.charAt(0));
        char firstCharOfSearchForLc = Character.toLowerCase(searchFor.charAt(0));

        if (Character.isWhitespace(firstCharOfSearchForLc) && searchMode.contains(SearchMode.SKIP_WHITE_SPACE)) {
            // Can't skip white spaces if first searchFor char is one
            searchMode = EnumSet.copyOf(searchMode);
            searchMode.remove(SearchMode.SKIP_WHITE_SPACE);
        }

        for (int i = startingPosition; i <= stopSearchingAt; i++) {
            i = indexOfNextChar(i, stopSearchingAt, searchIn, openingMarkers, closingMarkers, searchMode);

            if (i == -1) {
                return -1;
            }

            char c = searchIn.charAt(i);

            if (isCharEqualIgnoreCase(c, firstCharOfSearchForUc, firstCharOfSearchForLc) && startsWithIgnoreCase(searchIn, i, searchFor)) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Finds the position the next character from a string, possibly skipping white space, comments and text between markers.
     *
     * @param startingPosition
     *            the position to start the search from
     * @param stopPosition
     *            the position where to stop the search (inclusive)
     * @param searchIn
     *            the string to search in
     * @param openingMarkers
     *            characters which delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters which delimit the end of a text block to skip
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behavior of the search
     * @return the position where <code>searchFor</code> is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    private static int indexOfNextChar(int startingPosition, int stopPosition, String searchIn, String openingMarkers, String closingMarkers,
                                       Set<SearchMode> searchMode) {
        if (searchIn == null) {
            return -1;
        }

        int searchInLength = searchIn.length();
        if (startingPosition >= searchInLength) {
            return -1;
        }

        char c0 = Character.MIN_VALUE; // current char
        char c1 = searchIn.charAt(startingPosition); // lookahead(1)
        char c2 = startingPosition + 1 < searchInLength ? searchIn.charAt(startingPosition + 1) : Character.MIN_VALUE; // lookahead(2)

        for (int i = startingPosition; i <= stopPosition; i++) {
            c0 = c1;
            c1 = c2;
            c2 = i + 2 < searchInLength ? searchIn.charAt(i + 2) : Character.MIN_VALUE;

            boolean dashDashCommentImmediateEnd = false;
            int markerIndex = -1;
            if (searchMode.contains(SearchMode.ALLOW_BACKSLASH_ESCAPE) && c0 == '\\') {
                i++; // next char is escaped, skip it
                // reset lookahead
                c1 = c2;
                c2 = i + 2 < searchInLength ? searchIn.charAt(i + 2) : Character.MIN_VALUE;
            } else if (searchMode.contains(SearchMode.SKIP_BETWEEN_MARKERS) && (markerIndex = openingMarkers.indexOf(c0)) != -1) {
                // marker found, skip until closing, while being aware of nested markers if opening and closing markers are distinct
                int nestedMarkersCount = 0;
                char openingMarker = c0;
                char closingMarker = closingMarkers.charAt(markerIndex);
                while (++i <= stopPosition && ((c0 = searchIn.charAt(i)) != closingMarker || nestedMarkersCount != 0)) {
                    if (c0 == openingMarker) {
                        nestedMarkersCount++;
                    } else if (c0 == closingMarker) {
                        nestedMarkersCount--;
                    } else if (searchMode.contains(SearchMode.ALLOW_BACKSLASH_ESCAPE) && c0 == '\\') {
                        i++; // next char is escaped, skip it
                    }
                }
                // reset lookahead
                c1 = i + 1 < searchInLength ? searchIn.charAt(i + 1) : Character.MIN_VALUE;
                c2 = i + 2 < searchInLength ? searchIn.charAt(i + 2) : Character.MIN_VALUE;

            } else if (searchMode.contains(SearchMode.SKIP_BLOCK_COMMENTS) && c0 == '/' && c1 == '*') {
                if (c2 != '!') {
                    // comments block found, skip until end of block ("*/") (backslash escape doesn't work on comments)
                    i++; // move to next char ('*')
                    while (++i <= stopPosition
                      && (searchIn.charAt(i) != '*' || (i + 1 < searchInLength ? searchIn.charAt(i + 1) : Character.MIN_VALUE) != '/')) {
                        // continue
                    }
                    i++; // move to next char ('/')
                } else {
                    // special non-comments block found, move to end of opening marker ("/*![12345]")
                    i++; // move to next char ('*')
                    i++; // move to next char ('!')
                    // check if a 5 digits MySQL version reference follows, if so skip them
                    int j = 1;
                    for (; j <= NON_COMMENTS_MYSQL_VERSION_REF_LENGTH; j++) {
                        if (i + j >= searchInLength || !Character.isDigit(searchIn.charAt(i + j))) {
                            break;
                        }
                    }
                    if (j == NON_COMMENTS_MYSQL_VERSION_REF_LENGTH) {
                        i += NON_COMMENTS_MYSQL_VERSION_REF_LENGTH;
                    }
                }
                // reset lookahead
                c1 = i + 1 < searchInLength ? searchIn.charAt(i + 1) : Character.MIN_VALUE;
                c2 = i + 2 < searchInLength ? searchIn.charAt(i + 2) : Character.MIN_VALUE;
            } else if (searchMode.contains(SearchMode.SKIP_BLOCK_COMMENTS) && c0 == '*' && c1 == '/') {
                // special non-comments block closing marker ("*/") found - assume that if we get it here it's because it
                // belongs to a non-comments block ("/*!"), otherwise the query should be misspelled as nesting comments isn't allowed.
                i++; // move to next char ('/')
                // reset lookahead
                c1 = c2;
                c2 = i + 2 < searchInLength ? searchIn.charAt(i + 2) : Character.MIN_VALUE;
            } else if (searchMode.contains(SearchMode.SKIP_LINE_COMMENTS)
              && ((c0 == '-' && c1 == '-' && (Character.isWhitespace(c2) || (dashDashCommentImmediateEnd = c2 == ';') || c2 == Character.MIN_VALUE))
              || c0 == '#')) {
                if (dashDashCommentImmediateEnd) {
                    // comments line found but closed immediately by query delimiter marker
                    i++; // move to next char ('-')
                    i++; // move to next char (';')
                    // reset lookahead
                    c1 = i + 1 < searchInLength ? searchIn.charAt(i + 1) : Character.MIN_VALUE;
                    c2 = i + 2 < searchInLength ? searchIn.charAt(i + 2) : Character.MIN_VALUE;
                } else {
                    // comments line found, skip until eol (backslash escape doesn't work on comments)
                    while (++i <= stopPosition && (c0 = searchIn.charAt(i)) != '\n' && c0 != '\r') {
                        // continue
                    }
                    // reset lookahead
                    c1 = i + 1 < searchInLength ? searchIn.charAt(i + 1) : Character.MIN_VALUE;
                    if (c0 == '\r' && c1 == '\n') {
                        // \r\n sequence found
                        i++; // skip next char ('\n')
                        c1 = i + 1 < searchInLength ? searchIn.charAt(i + 1) : Character.MIN_VALUE;
                    }
                    c2 = i + 2 < searchInLength ? searchIn.charAt(i + 2) : Character.MIN_VALUE;
                }
            } else if (!searchMode.contains(SearchMode.SKIP_WHITE_SPACE) || !Character.isWhitespace(c0)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Trims identifier, removes quote chars from first and last positions
     * and replaces double occurrences of quote char from entire identifier,
     * i.e converts quoted identifier into form as it is stored in database.
     *
     * @param identifier
     * @param quoteChar
     *            ` or "
     * @return
     *         <ul>
     *         <li>null -> null</li>
     *         <li>abc -> abc</li>
     *         <li>`abc` -> abc</li>
     *         <li>`ab``c` -> ab`c</li>
     *         <li>`"ab`c"` -> "ab`c"</li>
     *         <li>`ab"c` -> ab"c</li>
     *         <li>"abc" -> abc</li>
     *         <li>"`ab""c`" -> `ab"c`</li>
     *         <li>"ab`c" -> ab`c</li>
     *         </ul>
     */
    public static String unQuoteIdentifier(String identifier, String quoteChar) {
        if (identifier == null) {
            return null;
        }
        identifier = identifier.trim();
        int quoteCharLength = quoteChar.length();
        if (quoteCharLength == 0 || " ".equals(quoteChar)) {
            return identifier;
        }

        // Check if the identifier is really quoted or if it simply contains quote chars in it (assuming that the value is a valid identifier).
        if (identifier.startsWith(quoteChar) && identifier.endsWith(quoteChar)) {
            String identifierQuoteTrimmed = identifier.substring(quoteCharLength, identifier.length() - quoteCharLength);
            // Check for pairs of quotes.
            int quoteCharPos = identifierQuoteTrimmed.indexOf(quoteChar);
            while (quoteCharPos >= 0) {
                int quoteCharNextExpectedPos = quoteCharPos + quoteCharLength;
                int quoteCharNextPosition = identifierQuoteTrimmed.indexOf(quoteChar, quoteCharNextExpectedPos);

                if (quoteCharNextPosition == quoteCharNextExpectedPos) {
                    quoteCharPos = identifierQuoteTrimmed.indexOf(quoteChar, quoteCharNextPosition + quoteCharLength);
                } else {
                    // Not a pair of quotes! Return as it is...
                    return identifier;
                }
            }
            return identifier.substring(quoteCharLength, (identifier.length() - quoteCharLength)).replaceAll(quoteChar + quoteChar, quoteChar);
        }
        return identifier;
    }

    /**
     * Splits stringToSplit into a list, using the given delimiter (skipping delimiters within quotes)
     *
     * @param stringToSplit
     *            the string to split
     * @param delimiter
     *            the string to split on
     * @param markers
     *            the marker for the beginning of a text block to skip, when looking for a delimiter
     * @param markerCloses
     *            the marker for the end of a text block to skip, when looking for a delimiter
     * @return the list of strings, split by delimiter
     *
     * @throws IllegalArgumentException
     */
    public static List<String> split(String stringToSplit, String delimiter, String markers, String markerCloses) {
        if (stringToSplit == null) {
            return new ArrayList<>();
        }
        if (delimiter == null) {
            throw new IllegalArgumentException();
        }

        int delimPos = 0;
        int currentPos = 0;
        List<String> splitTokens = new ArrayList<>();
        while ((delimPos = indexOfIgnoreCase(currentPos, stringToSplit, delimiter, markers, markerCloses, SEARCH_MODE__MRK_COM_WS)) != -1) {
            String token = stringToSplit.substring(currentPos, delimPos);
            splitTokens.add(token);
            currentPos = delimPos + 1;
        }

        if (currentPos < stringToSplit.length()) {
            String token = stringToSplit.substring(currentPos);
            splitTokens.add(token);
        }
        return splitTokens;
    }
}
