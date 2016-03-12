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

    /**
     * @param startingPosition
     * @param searchIn
     * @param searchFor
     * @return
     */
    public final static int indexOfIgnoreCase(int startingPosition, String searchIn,
        String searchFor) {
        if ((searchIn == null) || (searchFor == null) || startingPosition > searchIn.length()) {
            return -1;
        }

        int patternLength = searchFor.length();
        int stringLength = searchIn.length();
        int stopSearchingAt = stringLength - patternLength;

        if (patternLength == 0) {
            return -1;
        }

        // Brute force string pattern matching
        // Some locales don't follow upper-case rule, so need to check both
        char firstCharOfPatternUc = Character.toUpperCase(searchFor.charAt(0));
        char firstCharOfPatternLc = Character.toLowerCase(searchFor.charAt(0));

        // note, this also catches the case where patternLength > stringLength
        for (int i = startingPosition; i <= stopSearchingAt; i++) {
            if (isNotEqualIgnoreCharCase(searchIn, firstCharOfPatternUc, firstCharOfPatternLc, i)) {
                // find the first occurrence of the first character of searchFor in searchIn
                while (++i <= stopSearchingAt && (isNotEqualIgnoreCharCase(searchIn,
                    firstCharOfPatternUc, firstCharOfPatternLc, i)))
                    ;
            }

            if (i <= stopSearchingAt /* searchFor might be one character long! */) {
                // walk searchIn and searchFor in lock-step starting just past the first match,bail out if not
                // a match, or we've hit the end of searchFor...
                int j = i + 1;
                int end = j + patternLength - 1;
                for (int k = 1; j < end && (Character.toLowerCase(searchIn.charAt(j)) == Character
                    .toLowerCase(searchFor.charAt(k))
                    || Character.toUpperCase(searchIn.charAt(j)) == Character
                    .toUpperCase(searchFor.charAt(k))); j++, k++)
                    ;

                if (j == end) {
                    return i;
                }
            }
        }

        return -1;
    }

    /**
     * @param searchIn
     * @param firstCharOfPatternUc
     * @param firstCharOfPatternLc
     * @param i
     * @return
     */
    private final static boolean isNotEqualIgnoreCharCase(String searchIn,
        char firstCharOfPatternUc, char firstCharOfPatternLc, int i) {
        return Character.toLowerCase(searchIn.charAt(i)) != firstCharOfPatternLc
            && Character.toUpperCase(searchIn.charAt(i)) != firstCharOfPatternUc;
    }

}
