package com.dataliance.service.util;

import java.util.regex.*;

public class StringUtil
{
    private static Pattern urlPattern;
    
    public static String escape(final String origStr, final char[] escapeChars) {
        if (null == escapeChars) {
            return "";
        }
        final StringBuilder escapedStrBuf = new StringBuilder();
        for (int index = 0; index < origStr.length(); ++index) {
            final char origChar = origStr.charAt(index);
            for (final char escapeChar : escapeChars) {
                if (escapeChar == origChar) {
                    escapedStrBuf.append('\\');
                    break;
                }
            }
            escapedStrBuf.append(origChar);
        }
        return escapedStrBuf.toString();
    }
    
    public static String getUrlPrefix(final String url) {
        final Matcher matcher = StringUtil.urlPattern.matcher(url);
        String urlPrefix = null;
        if (matcher.matches()) {
            urlPrefix = matcher.group(1);
        }
        return urlPrefix;
    }
    
    static {
        StringUtil.urlPattern = Pattern.compile("(^http://[^/]+).*");
    }
}
