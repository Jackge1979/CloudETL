package com.dataliance.service.util;

import java.util.regex.*;
import java.io.*;

public class Decoder
{
    private static Pattern DECODE_CODE_PATTERN;
    
    public static String decode(final String stringInUnicode) {
        if (null == stringInUnicode) {
            return null;
        }
        final StringBuffer decodeStringBuffer = new StringBuffer();
        int preEndIndex = 0;
        final Matcher matcher = Decoder.DECODE_CODE_PATTERN.matcher(stringInUnicode);
        while (matcher.find()) {
            if (matcher.start() > preEndIndex) {
                decodeStringBuffer.append(stringInUnicode.subSequence(preEndIndex, matcher.start()));
            }
            String code = matcher.group(2);
            if (null == code) {
                code = matcher.group(1);
            }
            try {
                if (code.charAt(0) == 'x' && (code.length() == 3 || code.length() == 5)) {
                    final String codeValue = code.substring(1);
                    decodeStringBuffer.append(decode(Integer.valueOf(Integer.parseInt(codeValue, 16))));
                }
                else {
                    decodeStringBuffer.append(decode(Integer.valueOf(code)));
                }
            }
            catch (Exception e) {
                decodeStringBuffer.append(code);
                e.printStackTrace();
                System.err.println(String.format("&#%s is not a valid &# encoded chunk", code));
            }
            preEndIndex = matcher.end();
        }
        if (preEndIndex < stringInUnicode.length()) {
            decodeStringBuffer.append(stringInUnicode.substring(preEndIndex));
        }
        return decodeStringBuffer.toString();
    }
    
    private static String decode(final Integer encodeIntValue) throws UnsupportedEncodingException {
        final byte[] bytes = { (byte)(encodeIntValue >> 8 & 0xFF), (byte)(encodeIntValue & 0xFF) };
        return new String(bytes, "unicode");
    }
    
    static {
        Decoder.DECODE_CODE_PATTERN = Pattern.compile("&#(x[0-9a-zA-Z]{2,4});?|&#(\\d{1,5});?");
    }
}
