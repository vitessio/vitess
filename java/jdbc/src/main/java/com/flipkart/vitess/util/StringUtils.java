package com.flipkart.vitess.util;

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

}
