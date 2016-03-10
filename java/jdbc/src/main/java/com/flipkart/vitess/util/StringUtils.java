package com.flipkart.vitess.util;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Created by naveen.nahata on 05/02/16.
 */
public class StringUtils {

    private StringUtils() {
    }

    public static Boolean parseBoolean(String str) {
        boolean retval;
        if (str != null && str.equals("1"))
            retval = true;
        else if (str != null && str.equals("0"))
            retval = false;
        else
            retval = Boolean.valueOf(str);
        return retval;
    }

    public static Byte parseByte(String str) {
        try {
            Byte b;
            if (str == null || str.length() == 0)
                b = null;
            else
                b = Byte.valueOf(Byte.parseByte(str));
            return b;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static Short parseShort(String str) {
        try {
            Short s;
            if (str == null || str.length() == 0)
                s = null;
            else
                s = Short.valueOf(Short.parseShort(str));
            return s;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static Integer parseInt(String str) {
        try {
            Integer i;
            if (str == null || str.length() == 0)
                i = null;
            else
                i = Integer.valueOf(Integer.parseInt(str));
            return i;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static Long parseLong(String str) {
        try {
            Long l;
            if (str == null || str.length() == 0)
                l = null;
            else
                l = Long.valueOf(Long.parseLong(str));
            return l;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static Float parseFloat(String str) {
        try {
            Float f;
            if (str == null || str.length() == 0) {
                f = null;
            } else {
                str = str.replace(",", ".");
                f = Float.valueOf(Float.parseFloat(str));
            }
            return f;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static Double parseDouble(String str) {
        try {
            Double d;
            if (str == null || str.length() == 0) {
                d = null;
            } else {
                str = str.replace(",", ".");
                d = Double.valueOf(Double.parseDouble(str));
            }
            return d;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static byte[] parseBytes(String str) {
        try {
            byte[] b;
            if (str == null)
                b = null;
            else
                b = str.getBytes();
            return b;
        } catch (RuntimeException e) {
            return null;
        }
    }

    public static InputStream parseAsciiStream(String str) {
        return (str == null) ? null : new ByteArrayInputStream(str.getBytes());
    }

    private static boolean isNotEqualIgnoreCharCase(String searchIn, char firstCharOfPatternUc,
        char firstCharOfPatternLc, int i) {
        return Character.toLowerCase(searchIn.charAt(i)) != firstCharOfPatternLc
            && Character.toUpperCase(searchIn.charAt(i)) != firstCharOfPatternUc;
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
