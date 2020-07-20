/*
 *
 * Copyright (c) 2011, Xiufeng Liu (xiliu@cs.aau.dk) and the eGovMon Consortium
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 *
 */

package dk.aau.cs.rite.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class StringUtils {

        private static final int BYTE_RANGE = (1 + Byte.MAX_VALUE) - Byte.MIN_VALUE;

        private static byte[] allBytes = new byte[BYTE_RANGE];

        private static char[] byteToChars = new char[BYTE_RANGE];

        private static Method toPlainStringMethod;

        static final int WILD_COMPARE_MATCH_NO_WILD = 0;

        static final int WILD_COMPARE_MATCH_WITH_WILD = 1;

        static final int WILD_COMPARE_NO_MATCH = -1;

        static {
                for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
                        allBytes[i - Byte.MIN_VALUE] = (byte) i;
                }

                String allBytesString = new String(allBytes, 0, Byte.MAX_VALUE
                                - Byte.MIN_VALUE);

                int allBytesStringLen = allBytesString.length();

                for (int i = 0; (i < (Byte.MAX_VALUE - Byte.MIN_VALUE))
                                && (i < allBytesStringLen); i++) {
                        byteToChars[i] = allBytesString.charAt(i);
                }

                try {
                        toPlainStringMethod = BigDecimal.class.getMethod("toPlainString",
                                        new Class[0]);
                } catch (NoSuchMethodException nsme) {
                        // that's okay, we fallback to .toString()
                }
        }

        public static String rtrim(String str, String ch) {
    		if (str == null)
    			return "";
    		int idx = str.lastIndexOf(ch);
    		if (idx != -1)
    			return str.substring(0, idx);
    		else
    			return str;
    	}
        
        /**
         * Takes care of the fact that Sun changed the output of
         * BigDecimal.toString() between JDK-1.4 and JDK 5
         *
         * @param decimal
         *            the big decimal to stringify
         *
         * @return a string representation of 'decimal'
         */

        /**
         * Dumps the given bytes to STDOUT as a hex dump (up to length bytes).
         *
         * @param byteBuffer
         *            the data to print as hex
         * @param length
         *            the number of bytes to print
         *
         * @return ...
         */
        public static final String dumpAsHex(byte[] byteBuffer, int length) {
                StringBuffer outputBuf = new StringBuffer(length * 4);

                int p = 0;
                int rows = length / 8;

                for (int i = 0; (i < rows) && (p < length); i++) {
                        int ptemp = p;

                        for (int j = 0; j < 8; j++) {
                                String hexVal = Integer.toHexString(byteBuffer[ptemp] & 0xff);

                                if (hexVal.length() == 1) {
                                        hexVal = "0" + hexVal; //$NON-NLS-1$
                                }

                                outputBuf.append(hexVal + " "); //$NON-NLS-1$
                                ptemp++;
                        }

                        outputBuf.append("    "); //$NON-NLS-1$

                        for (int j = 0; j < 8; j++) {
                                int b = 0xff & byteBuffer[p];

                                if (b > 32 && b < 127) {
                                        outputBuf.append((char) b + " "); //$NON-NLS-1$
                                } else {
                                        outputBuf.append(". "); //$NON-NLS-1$
                                }

                                p++;
                        }

                        outputBuf.append("\n"); //$NON-NLS-1$
                }

                int n = 0;

                for (int i = p; i < length; i++) {
                        String hexVal = Integer.toHexString(byteBuffer[i] & 0xff);

                        if (hexVal.length() == 1) {
                                hexVal = "0" + hexVal; //$NON-NLS-1$
                        }

                        outputBuf.append(hexVal + " "); //$NON-NLS-1$
                        n++;
                }

                for (int i = n; i < 8; i++) {
                        outputBuf.append("   "); //$NON-NLS-1$
                }

                outputBuf.append("    "); //$NON-NLS-1$

                for (int i = p; i < length; i++) {
                        int b = 0xff & byteBuffer[i];

                        if (b > 32 && b < 127) {
                                outputBuf.append((char) b + " "); //$NON-NLS-1$
                        } else {
                                outputBuf.append(". "); //$NON-NLS-1$
                        }
                }

                outputBuf.append("\n"); //$NON-NLS-1$

                return outputBuf.toString();
        }

        private static boolean endsWith(byte[] dataFrom, String suffix) {
                for (int i = 1; i <= suffix.length(); i++) {
                        int dfOffset = dataFrom.length - i;
                        int suffixOffset = suffix.length() - i;
                        if (dataFrom[dfOffset] != suffix.charAt(suffixOffset)) {
                                return false;
                        }
                }
                return true;
        }

        /**
         * Unfortunately, SJIS has 0x5c as a high byte in some of its double-byte
         * characters, so we need to escape it.
         *
         * @param origBytes
         *            the original bytes in SJIS format
         * @param origString
         *            the string that had .getBytes() called on it
         * @param offset
         *            where to start converting from
         * @param length
         *            how many characters to convert.
         *
         * @return byte[] with 0x5c escaped
         */
        public static byte[] escapeEasternUnicodeByteStream(byte[] origBytes,
                        String origString, int offset, int length) {
                if ((origBytes == null) || (origBytes.length == 0)) {
                        return origBytes;
                }

                int bytesLen = origBytes.length;
                int bufIndex = 0;
                int strIndex = 0;

                ByteArrayOutputStream bytesOut = new ByteArrayOutputStream(bytesLen);

                while (true) {
                        if (origString.charAt(strIndex) == '\\') {
                                // write it out as-is
                                bytesOut.write(origBytes[bufIndex++]);

                                // bytesOut.write(origBytes[bufIndex++]);
                        } else {
                                // Grab the first byte
                                int loByte = origBytes[bufIndex];

                                if (loByte < 0) {
                                        loByte += 256; // adjust for signedness/wrap-around
                                }

                                // We always write the first byte
                                bytesOut.write(loByte);

                                //
                                // The codepage characters in question exist between
                                // 0x81-0x9F and 0xE0-0xFC...
                                //
                                // See:
                                //
                                // http://www.microsoft.com/GLOBALDEV/Reference/dbcs/932.htm
                                //
                                // Problematic characters in GBK
                                //
                                // U+905C : CJK UNIFIED IDEOGRAPH
                                //
                                // Problematic characters in Big5
                                //
                                // B9F0 = U+5C62 : CJK UNIFIED IDEOGRAPH
                                //
                                if (loByte >= 0x80) {
                                        if (bufIndex < (bytesLen - 1)) {
                                                int hiByte = origBytes[bufIndex + 1];

                                                if (hiByte < 0) {
                                                        hiByte += 256; // adjust for signedness/wrap-around
                                                }

                                                // write the high byte here, and increment the index
                                                // for the high byte
                                                bytesOut.write(hiByte);
                                                bufIndex++;

                                                // escape 0x5c if necessary
                                                if (hiByte == 0x5C) {
                                                        bytesOut.write(hiByte);
                                                }
                                        }
                                } else if (loByte == 0x5c) {
                                        if (bufIndex < (bytesLen - 1)) {
                                                int hiByte = origBytes[bufIndex + 1];

                                                if (hiByte < 0) {
                                                        hiByte += 256; // adjust for signedness/wrap-around
                                                }

                                                if (hiByte == 0x62) {
                                                        // we need to escape the 0x5c
                                                        bytesOut.write(0x5c);
                                                        bytesOut.write(0x62);
                                                        bufIndex++;
                                                }
                                        }
                                }

                                bufIndex++;
                        }

                        if (bufIndex >= bytesLen) {
                                // we're done
                                break;
                        }

                        strIndex++;
                }

                return bytesOut.toByteArray();
        }

        /**
         * Returns the first non whitespace char, converted to upper case
         *
         * @param searchIn
         *            the string to search in
         *
         * @return the first non-whitespace character, upper cased.
         */
        public static char firstNonWsCharUc(String searchIn) {
                return firstNonWsCharUc(searchIn, 0);
        }

        public static char firstNonWsCharUc(String searchIn, int startAt) {
                if (searchIn == null) {
                        return 0;
                }

                int length = searchIn.length();

                for (int i = startAt; i < length; i++) {
                        char c = searchIn.charAt(i);

                        if (!Character.isWhitespace(c)) {
                                return Character.toUpperCase(c);
                        }
                }

                return 0;
        }

        public static char firstAlphaCharUc(String searchIn, int startAt) {
                if (searchIn == null) {
                        return 0;
                }

                int length = searchIn.length();

                for (int i = startAt; i < length; i++) {
                        char c = searchIn.charAt(i);

                        if (Character.isLetter(c)) {
                                return Character.toUpperCase(c);
                        }
                }

                return 0;
        }

        /**
         * Adds '+' to decimal numbers that are positive (MySQL doesn't understand
         * them otherwise
         *
         * @param dString
         *            The value as a string
         *
         * @return String the string with a '+' added (if needed)
         */
        public static final String fixDecimalExponent(String dString) {
                int ePos = dString.indexOf("E"); //$NON-NLS-1$

                if (ePos == -1) {
                        ePos = dString.indexOf("e"); //$NON-NLS-1$
                }

                if (ePos != -1) {
                        if (dString.length() > (ePos + 1)) {
                                char maybeMinusChar = dString.charAt(ePos + 1);

                                if (maybeMinusChar != '-' && maybeMinusChar != '+') {
                                        StringBuffer buf = new StringBuffer(dString.length() + 1);
                                        buf.append(dString.substring(0, ePos + 1));
                                        buf.append('+');
                                        buf.append(dString.substring(ePos + 1, dString.length()));
                                        dString = buf.toString();
                                }
                        }
                }

                return dString;
        }



  
        public static int getInt(byte[] buf, int offset, int endPos) throws NumberFormatException {
                int base = 10;

                int s = offset;

                /* Skip white space. */
                while (Character.isWhitespace((char) buf[s]) && (s < endPos)) {
                        ++s;
                }

                if (s == endPos) {
                        throw new NumberFormatException(new String(buf));
                }

                /* Check for a sign. */
                boolean negative = false;

                if ((char) buf[s] == '-') {
                        negative = true;
                        ++s;
                } else if ((char) buf[s] == '+') {
                        ++s;
                }

                /* Save the pointer so we can check later if anything happened. */
                int save = s;

                int cutoff = Integer.MAX_VALUE / base;
                int cutlim = (Integer.MAX_VALUE % base);

                if (negative) {
                        cutlim++;
                }

                boolean overflow = false;

                int i = 0;

                for (; s < endPos; s++) {
                        char c = (char) buf[s];

                        if (Character.isDigit(c)) {
                                c -= '0';
                        } else if (Character.isLetter(c)) {
                                c = (char) (Character.toUpperCase(c) - 'A' + 10);
                        } else {
                                break;
                        }

                        if (c >= base) {
                                break;
                        }

                        /* Check for overflow. */
                        if ((i > cutoff) || ((i == cutoff) && (c > cutlim))) {
                                overflow = true;
                        } else {
                                i *= base;
                                i += c;
                        }
                }

                if (s == save) {
                        throw new NumberFormatException(new String(buf));
                }

                if (overflow) {
                        throw new NumberFormatException(new String(buf));
                }

                /* Return the result of the appropriate sign. */
                return (negative ? (-i) : i);
        }

        public static int getInt(byte[] buf) throws NumberFormatException {
                return getInt(buf, 0, buf.length);
        }

        public static long getLong(byte[] buf) throws NumberFormatException {
                return getLong(buf, 0, buf.length);
        }

        public static long getLong(byte[] buf, int offset, int endpos) throws NumberFormatException {
                int base = 10;

                int s = offset;

                /* Skip white space. */
                while (Character.isWhitespace((char) buf[s]) && (s < endpos)) {
                        ++s;
                }

                if (s == endpos) {
                        throw new NumberFormatException(new String(buf));
                }

                /* Check for a sign. */
                boolean negative = false;

                if ((char) buf[s] == '-') {
                        negative = true;
                        ++s;
                } else if ((char) buf[s] == '+') {
                        ++s;
                }

                /* Save the pointer so we can check later if anything happened. */
                int save = s;

                long cutoff = Long.MAX_VALUE / base;
                long cutlim = (int) (Long.MAX_VALUE % base);

                if (negative) {
                        cutlim++;
                }

                boolean overflow = false;
                long i = 0;

                for (; s < endpos; s++) {
                        char c = (char) buf[s];

                        if (Character.isDigit(c)) {
                                c -= '0';
                        } else if (Character.isLetter(c)) {
                                c = (char) (Character.toUpperCase(c) - 'A' + 10);
                        } else {
                                break;
                        }

                        if (c >= base) {
                                break;
                        }

                        /* Check for overflow. */
                        if ((i > cutoff) || ((i == cutoff) && (c > cutlim))) {
                                overflow = true;
                        } else {
                                i *= base;
                                i += c;
                        }
                }

                if (s == save) {
                        throw new NumberFormatException(new String(buf));
                }

                if (overflow) {
                        throw new NumberFormatException(new String(buf));
                }

                /* Return the result of the appropriate sign. */
                return (negative ? (-i) : i);
        }

        public static short getShort(byte[] buf) throws NumberFormatException {
                short base = 10;

                int s = 0;

                /* Skip white space. */
                while (Character.isWhitespace((char) buf[s]) && (s < buf.length)) {
                        ++s;
                }

                if (s == buf.length) {
                        throw new NumberFormatException(new String(buf));
                }

                /* Check for a sign. */
                boolean negative = false;

                if ((char) buf[s] == '-') {
                        negative = true;
                        ++s;
                } else if ((char) buf[s] == '+') {
                        ++s;
                }

                /* Save the pointer so we can check later if anything happened. */
                int save = s;

                short cutoff = (short) (Short.MAX_VALUE / base);
                short cutlim = (short) (Short.MAX_VALUE % base);

                if (negative) {
                        cutlim++;
                }

                boolean overflow = false;
                short i = 0;

                for (; s < buf.length; s++) {
                        char c = (char) buf[s];

                        if (Character.isDigit(c)) {
                                c -= '0';
                        } else if (Character.isLetter(c)) {
                                c = (char) (Character.toUpperCase(c) - 'A' + 10);
                        } else {
                                break;
                        }

                        if (c >= base) {
                                break;
                        }

                        /* Check for overflow. */
                        if ((i > cutoff) || ((i == cutoff) && (c > cutlim))) {
                                overflow = true;
                        } else {
                                i *= base;
                                i += c;
                        }
                }

                if (s == save) {
                        throw new NumberFormatException(new String(buf));
                }

                if (overflow) {
                        throw new NumberFormatException(new String(buf));
                }

                /* Return the result of the appropriate sign. */
                return (negative ? (short) -i : (short) i);
        }

        public final static int indexOfIgnoreCase(int startingPosition,
                        String searchIn, String searchFor) {
                if ((searchIn == null) || (searchFor == null)
                                || startingPosition > searchIn.length()) {
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
            if (isNotEqualIgnoreCharCase(searchIn, firstCharOfPatternUc,
                                        firstCharOfPatternLc, i)) {
                // find the first occurrence of the first character of searchFor in searchIn
                while (++i <= stopSearchingAt && (isNotEqualIgnoreCharCase(searchIn, firstCharOfPatternUc,
                                                firstCharOfPatternLc, i)));
            }

            if (i <= stopSearchingAt /* searchFor might be one character long! */) {
                // walk searchIn and searchFor in lock-step starting just past the first match,bail out if not
                // a match, or we've hit the end of searchFor...
                int j = i + 1;
                int end = j + patternLength - 1;
                for (int k = 1; j < end && (Character.toLowerCase(searchIn.charAt(j)) ==
                        Character.toLowerCase(searchFor.charAt(k)) || Character.toUpperCase(searchIn.charAt(j)) ==
                        Character.toUpperCase(searchFor.charAt(k))); j++, k++);

                if (j == end) {
                    return i;
                }
            }
        }

        return -1;
        }

        private final static boolean isNotEqualIgnoreCharCase(String searchIn,
                        char firstCharOfPatternUc, char firstCharOfPatternLc, int i) {
                return Character.toLowerCase(searchIn.charAt(i)) != firstCharOfPatternLc && Character.toUpperCase(searchIn.charAt(i)) != firstCharOfPatternUc;
        }



        public final static int indexOfIgnoreCase(String searchIn, String searchFor) {
                return indexOfIgnoreCase(0, searchIn, searchFor);
        }

        public static int indexOfIgnoreCaseRespectMarker(int startAt, String src,
                        String target, String marker, String markerCloses,
                        boolean allowBackslashEscapes) {
                char contextMarker = Character.MIN_VALUE;
                boolean escaped = false;
                int markerTypeFound = 0;
                int srcLength = src.length();
                int ind = 0;

                for (int i = startAt; i < srcLength; i++) {
                        char c = src.charAt(i);

                        if (allowBackslashEscapes && c == '\\') {
                                escaped = !escaped;
                        } else if (c == markerCloses.charAt(markerTypeFound) && !escaped) {
                                contextMarker = Character.MIN_VALUE;
                        } else if ((ind = marker.indexOf(c)) != -1 && !escaped
                                        && contextMarker == Character.MIN_VALUE) {
                                markerTypeFound = ind;
                                contextMarker = c;
                        } else if ((Character.toUpperCase(c) == Character.toUpperCase(target.charAt(0)) ||
                                        Character.toLowerCase(c) == Character.toLowerCase(target.charAt(0))) && !escaped
                                        && contextMarker == Character.MIN_VALUE) {
                                if (startsWithIgnoreCase(src, i, target))
                                        return i;
                        }
                }

                return -1;

        }

        public static int indexOfIgnoreCaseRespectQuotes(int startAt, String src,
                        String target, char quoteChar, boolean allowBackslashEscapes) {
                char contextMarker = Character.MIN_VALUE;
                boolean escaped = false;

                int srcLength = src.length();

                for (int i = startAt; i < srcLength; i++) {
                        char c = src.charAt(i);

                        if (allowBackslashEscapes && c == '\\') {
                                escaped = !escaped;
                        } else if (c == contextMarker && !escaped) {
                                contextMarker = Character.MIN_VALUE;
                        } else if (c == quoteChar && !escaped
                                        && contextMarker == Character.MIN_VALUE) {
                                contextMarker = c;
                        // This test looks complex, but remember that in certain locales, upper case
                        // of two different codepoints coverts to same codepoint, and vice-versa.
                        } else if ((Character.toUpperCase(c) == Character.toUpperCase(target.charAt(0)) ||
                                        Character.toLowerCase(c) == Character.toLowerCase(target.charAt(0))) && !escaped
                                        && contextMarker == Character.MIN_VALUE) {
                                if (startsWithIgnoreCase(src, i, target))
                                        return i;
                        }
                }

                return -1;

        }

  
        public static final List split(String stringToSplit, String delimitter,
                        boolean trim) {
                if (stringToSplit == null) {
                        return new ArrayList();
                }

                if (delimitter == null) {
                        throw new IllegalArgumentException();
                }

                StringTokenizer tokenizer = new StringTokenizer(stringToSplit, delimitter, false);

                List splitTokens = new ArrayList(tokenizer.countTokens());

                while (tokenizer.hasMoreTokens()) {
                        String token = tokenizer.nextToken();

                        if (trim) {
                                token = token.trim();
                        }

                        splitTokens.add(token);
                }

                return splitTokens;
        }

    	public static final String[] splitToArray(String stringToSplit, String delimitter, boolean trim) {
    		if (stringToSplit == null) {
    			return new String[] {};
    		}
    		if (delimitter == null) {
    			throw new IllegalArgumentException();
    		}
    		StringTokenizer tokenizer = new StringTokenizer(stringToSplit, delimitter, false);
    		int count = tokenizer.countTokens();
    		String[] splitTokens = new String[count];
    		for (int i = 0; i < count; ++i) {
    			String token = tokenizer.nextToken();
    			if (trim) {
    				token = token.trim();
    			}
    			splitTokens[i] = token;
    		}
    		return splitTokens;
    	}
        
   
        public static final List split(String stringToSplit, String delimiter,
                        String markers, String markerCloses, boolean trim) {
                if (stringToSplit == null) {
                        return new ArrayList();
                }

                if (delimiter == null) {
                        throw new IllegalArgumentException();
                }

                int delimPos = 0;
                int currentPos = 0;

                List splitTokens = new ArrayList();

                while ((delimPos = indexOfIgnoreCaseRespectMarker(currentPos,
                                stringToSplit, delimiter, markers, markerCloses, false)) != -1) {
                        String token = stringToSplit.substring(currentPos, delimPos);

                        if (trim) {
                                token = token.trim();
                        }

                        splitTokens.add(token);
                        currentPos = delimPos + 1;
                }

                if (currentPos < stringToSplit.length()) {
                        String token = stringToSplit.substring(currentPos);

                        if (trim) {
                                token = token.trim();
                        }

                        splitTokens.add(token);
                }

                return splitTokens;
        }

        private static boolean startsWith(byte[] dataFrom, String chars) {
                for (int i = 0; i < chars.length(); i++) {
                        if (dataFrom[i] != chars.charAt(i)) {
                                return false;
                        }
                }
                return true;
        }

 
        public static boolean startsWithIgnoreCase(String searchIn, int startAt,
                        String searchFor) {
                return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor
                                .length());
        }

        /**
         * Determines whether or not the string 'searchIn' contains the string
         * 'searchFor', dis-regarding case. Shorthand for a String.regionMatch(...)
         *
         * @param searchIn
         *            the string to search in
         * @param searchFor
         *            the string to search for
         *
         * @return whether searchIn starts with searchFor, ignoring case
         */
        public static boolean startsWithIgnoreCase(String searchIn, String searchFor) {
                return startsWithIgnoreCase(searchIn, 0, searchFor);
        }

        /**
         * Determines whether or not the sting 'searchIn' contains the string
         * 'searchFor', disregarding case,leading whitespace and non-alphanumeric
         * characters.
         *
         * @param searchIn
         *            the string to search in
         * @param searchFor
         *            the string to search for
         *
         * @return true if the string starts with 'searchFor' ignoring whitespace
         */
        public static boolean startsWithIgnoreCaseAndNonAlphaNumeric(
                        String searchIn, String searchFor) {
                if (searchIn == null) {
                        return searchFor == null;
                }

                int beginPos = 0;

                int inLength = searchIn.length();

                for (beginPos = 0; beginPos < inLength; beginPos++) {
                        char c = searchIn.charAt(beginPos);

                        if (Character.isLetterOrDigit(c)) {
                                break;
                        }
                }

                return startsWithIgnoreCase(searchIn, beginPos, searchFor);
        }

        /**
         * Determines whether or not the sting 'searchIn' contains the string
         * 'searchFor', disregarding case and leading whitespace
         *
         * @param searchIn
         *            the string to search in
         * @param searchFor
         *            the string to search for
         *
         * @return true if the string starts with 'searchFor' ignoring whitespace
         */
        public static boolean startsWithIgnoreCaseAndWs(String searchIn,
                        String searchFor) {
                return startsWithIgnoreCaseAndWs(searchIn, searchFor, 0);
        }

        /**
         * Determines whether or not the sting 'searchIn' contains the string
         * 'searchFor', disregarding case and leading whitespace
         *
         * @param searchIn
         *            the string to search in
         * @param searchFor
         *            the string to search for
         * @param beginPos
         *            where to start searching
         *
         * @return true if the string starts with 'searchFor' ignoring whitespace
         */

        public static boolean startsWithIgnoreCaseAndWs(String searchIn,
                        String searchFor, int beginPos) {
                if (searchIn == null) {
                        return searchFor == null;
                }

                int inLength = searchIn.length();

                for (; beginPos < inLength; beginPos++) {
                        if (!Character.isWhitespace(searchIn.charAt(beginPos))) {
                                break;
                        }
                }

                return startsWithIgnoreCase(searchIn, beginPos, searchFor);
        }

  
        public static byte[] stripEnclosure(byte[] source, String prefix,
                        String suffix) {
                if (source.length >= prefix.length() + suffix.length()
                                && startsWith(source, prefix) && endsWith(source, suffix)) {

                        int totalToStrip = prefix.length() + suffix.length();
                        int enclosedLength = source.length - totalToStrip;
                        byte[] enclosed = new byte[enclosedLength];

                        int startPos = prefix.length();
                        int numToCopy = enclosed.length;
                        System.arraycopy(source, startPos, enclosed, 0, numToCopy);

                        return enclosed;
                }
                return source;
        }


        public static final String toAsciiString(byte[] buffer) {
                return toAsciiString(buffer, 0, buffer.length);
        }


        public static final String toAsciiString(byte[] buffer, int startPos,
                        int length) {
                char[] charArray = new char[length];
                int readpoint = startPos;

                for (int i = 0; i < length; i++) {
                        charArray[i] = (char) buffer[readpoint];
                        readpoint++;
                }

                return new String(charArray);
        }


        public static int wildCompare(String searchIn, String searchForWildcard) {
                if ((searchIn == null) || (searchForWildcard == null)) {
                        return WILD_COMPARE_NO_MATCH;
                }

                if (searchForWildcard.equals("%")) { //$NON-NLS-1$

                        return WILD_COMPARE_MATCH_WITH_WILD;
                }

                int result = WILD_COMPARE_NO_MATCH; /* Not found, using wildcards */

                char wildcardMany = '%';
                char wildcardOne = '_';
                char wildcardEscape = '\\';

                int searchForPos = 0;
                int searchForEnd = searchForWildcard.length();

                int searchInPos = 0;
                int searchInEnd = searchIn.length();

                while (searchForPos != searchForEnd) {
                        char wildstrChar = searchForWildcard.charAt(searchForPos);

                        while ((searchForWildcard.charAt(searchForPos) != wildcardMany)
                                        && (wildstrChar != wildcardOne)) {
                                if ((searchForWildcard.charAt(searchForPos) == wildcardEscape)
                                                && ((searchForPos + 1) != searchForEnd)) {
                                        searchForPos++;
                                }

                                if ((searchInPos == searchInEnd)
                                                || (Character.toUpperCase(searchForWildcard
                                                                .charAt(searchForPos++)) != Character
                                                                .toUpperCase(searchIn.charAt(searchInPos++)))) {
                                        return WILD_COMPARE_MATCH_WITH_WILD; /* No match */
                                }

                                if (searchForPos == searchForEnd) {
                                        return ((searchInPos != searchInEnd) ? WILD_COMPARE_MATCH_WITH_WILD
                                                        : WILD_COMPARE_MATCH_NO_WILD); /*
                                                                                                                         * Match if both are
                                                                                                                         * at end
                                                                                                                         */
                                }

                                result = WILD_COMPARE_MATCH_WITH_WILD; /* Found an anchor char */
                        }

                        if (searchForWildcard.charAt(searchForPos) == wildcardOne) {
                                do {
                                        if (searchInPos == searchInEnd) { /*
                                                                                                                 * Skip one char if
                                                                                                                 * possible
                                                                                                                 */

                                                return (result);
                                        }

                                        searchInPos++;
                                } while ((++searchForPos < searchForEnd)
                                                && (searchForWildcard.charAt(searchForPos) == wildcardOne));

                                if (searchForPos == searchForEnd) {
                                        break;
                                }
                        }

                        if (searchForWildcard.charAt(searchForPos) == wildcardMany) { /*
                                                                                                                                                         * Found
                                                                                                                                                         * w_many
                                                                                                                                                         */

                                char cmp;

                                searchForPos++;

                                /* Remove any '%' and '_' from the wild search string */
                                for (; searchForPos != searchForEnd; searchForPos++) {
                                        if (searchForWildcard.charAt(searchForPos) == wildcardMany) {
                                                continue;
                                        }

                                        if (searchForWildcard.charAt(searchForPos) == wildcardOne) {
                                                if (searchInPos == searchInEnd) {
                                                        return (WILD_COMPARE_NO_MATCH);
                                                }

                                                searchInPos++;

                                                continue;
                                        }

                                        break; /* Not a wild character */
                                }

                                if (searchForPos == searchForEnd) {
                                        return WILD_COMPARE_MATCH_NO_WILD; /* Ok if w_many is last */
                                }

                                if (searchInPos == searchInEnd) {
                                        return WILD_COMPARE_NO_MATCH;
                                }

                                if (((cmp = searchForWildcard.charAt(searchForPos)) == wildcardEscape)
                                                && ((searchForPos + 1) != searchForEnd)) {
                                        cmp = searchForWildcard.charAt(++searchForPos);
                                }

                                searchForPos++;

                                do {
                                        while ((searchInPos != searchInEnd)
                                                        && (Character.toUpperCase(searchIn
                                                                        .charAt(searchInPos)) != Character
                                                                        .toUpperCase(cmp)))
                                                searchInPos++;

                                        if (searchInPos++ == searchInEnd) {
                                                return WILD_COMPARE_NO_MATCH;
                                        }

                                        {
                                                int tmp = wildCompare(searchIn, searchForWildcard);

                                                if (tmp <= 0) {
                                                        return (tmp);
                                                }
                                        }
                                } while ((searchInPos != searchInEnd)
                                                && (searchForWildcard.charAt(0) != wildcardMany));

                                return WILD_COMPARE_NO_MATCH;
                        }
                }

                return ((searchInPos != searchInEnd) ? WILD_COMPARE_MATCH_WITH_WILD
                                : WILD_COMPARE_MATCH_NO_WILD);
        }


        public static int lastIndexOf(byte[] s, char c) {
                if (s == null) {
                        return -1;
                }

                for (int i = s.length - 1; i >= 0; i--) {
                        if (s[i] == c) {
                                return i;
                        }
                }

                return -1;
        }

        public static int indexOf(byte[] s, char c) {
                if (s == null) {
                        return -1;
                }

                int length = s.length;

                for (int i = 0; i < length; i++) {
                        if (s[i] == c) {
                                return i;
                        }
                }

                return -1;
        }

        public static boolean isNullOrEmpty(String toTest) {
                return (toTest == null || toTest.length() == 0);
        }

  
        public static String stripComments(String src, String stringOpens,
                        String stringCloses, boolean slashStarComments,
                        boolean slashSlashComments, boolean hashComments,
                        boolean dashDashComments) {
                if (src == null) {
                        return null;
                }

                StringBuffer buf = new StringBuffer(src.length());

                // It's just more natural to deal with this as a stream
                // when parsing..This code is currently only called when
                // parsing the kind of metadata that developers are strongly
                // recommended to cache anyways, so we're not worried
                // about the _1_ extra object allocation if it cleans
                // up the code

                StringReader sourceReader = new StringReader(src);

                int contextMarker = Character.MIN_VALUE;
                boolean escaped = false;
                int markerTypeFound = -1;

                int ind = 0;

                int currentChar = 0;

                try {
                        while ((currentChar = sourceReader.read()) != -1) {

                                if (false && currentChar == '\\') {
                                        escaped = !escaped;
                                } else if (markerTypeFound != -1 && currentChar == stringCloses.charAt(markerTypeFound)
                                                && !escaped) {
                                        contextMarker = Character.MIN_VALUE;
                                        markerTypeFound = -1;
                                } else if ((ind = stringOpens.indexOf(currentChar)) != -1
                                                && !escaped && contextMarker == Character.MIN_VALUE) {
                                        markerTypeFound = ind;
                                        contextMarker = currentChar;
                                }

                                if (contextMarker == Character.MIN_VALUE && currentChar == '/'
                                                && (slashSlashComments || slashStarComments)) {
                                        currentChar = sourceReader.read();
                                        if (currentChar == '*' && slashStarComments) {
                                                int prevChar = 0;
                                                while ((currentChar = sourceReader.read()) != '/'
                                                                || prevChar != '*') {
                                                        if (currentChar == '\r') {

                                                                currentChar = sourceReader.read();
                                                                if (currentChar == '\n') {
                                                                        currentChar = sourceReader.read();
                                                                }
                                                        } else {
                                                                if (currentChar == '\n') {

                                                                        currentChar = sourceReader.read();
                                                                }
                                                        }
                                                        if (currentChar < 0)
                                                                break;
                                                        prevChar = currentChar;
                                                }
                                                continue;
                                        } else if (currentChar == '/' && slashSlashComments) {
                                                while ((currentChar = sourceReader.read()) != '\n'
                                                                && currentChar != '\r' && currentChar >= 0)
                                                        ;
                                        }
                                } else if (contextMarker == Character.MIN_VALUE
                                                && currentChar == '#' && hashComments) {
                                        // Slurp up everything until the newline
                                        while ((currentChar = sourceReader.read()) != '\n'
                                                        && currentChar != '\r' && currentChar >= 0)
                                                ;
                                } else if (contextMarker == Character.MIN_VALUE
                                                && currentChar == '-' && dashDashComments) {
                                        currentChar = sourceReader.read();

                                        if (currentChar == -1 || currentChar != '-') {
                                                buf.append('-');

                                                if (currentChar != -1) {
                                                        buf.append(currentChar);
                                                }

                                                continue;
                                        }

                                        // Slurp up everything until the newline

                                        while ((currentChar = sourceReader.read()) != '\n'
                                                        && currentChar != '\r' && currentChar >= 0)
                                                ;
                                }

                                if (currentChar != -1) {
                                        buf.append((char) currentChar);
                                }
                        }
                } catch (IOException ioEx) {
                        // we'll never see this from a StringReader
                }

                return buf.toString();
        }

        public static final boolean isEmptyOrWhitespaceOnly(String str) {
                if (str == null || str.length() == 0) {
                        return true;
                }

                int length = str.length();

                for (int i = 0; i < length; i++) {
                        if (!Character.isWhitespace(str.charAt(i))) {
                                return false;
                        }
                }

                return true;
        }
        
    	public static String mapEmpty2Null(String str) {

    		if (str == null || "null".equalsIgnoreCase(str.trim()) || "?".equals(str.trim())||  "".equals(str.trim()))
    			return null;
    		else
    			return str;
    	}
    	

    	
   	 public static String rtrimSpecialChar(String str, char[] chars) {
			if (str==null) return null;
			int i = str.length()-1;
			for (; i>=0; --i){
				if (!contains(chars, str.charAt(i))){
					break;
				}
			}
			return str.substring(0, i+1);
		  }

	 
	 public static String trimSpecialChar(String str, char[] chars) {
			if (str==null) return null;
			String subStr = null;
			for (int i=0; i<str.length(); ++i){
				if (contains(chars, str.charAt(i))){
					continue;
				}
				subStr = str.substring(i);
				break;
			}
			return rtrimSpecialChar(subStr, chars);
		  }
	 
	 
	 public static boolean contains(char[] chars, char ch){
		 for (int c : chars){
			 if (ch==c) return true;
		 }
		 return false;
	 }
	 
	 public static void main(String[]args){
		 String line = "aa||b|||1";
		String[] arr = StringUtils.splitToArray(line, "|", false);
		for (String s : arr){
			System.out.println(s);
		}
	 }
}
