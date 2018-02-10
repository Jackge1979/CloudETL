package com.dataliance.util;

import java.io.*;
import java.util.logging.Logger;

import org.slf4j.*;

public class StringUtil
{
    public static final String UTF8_ENCODING = "UTF-8";
    public static final Logger LOG;
    
    public static boolean isEmpty(final String src) {
        return src == null || src.trim().isEmpty();
    }
    
    public static boolean isEmpty(final Object src) {
        return src == null || src.toString().trim().isEmpty();
    }
    
    public static String toString(final Object src) {
        return isEmpty(src) ? "" : src.toString();
    }
    
    public static int toInt(final String src) {
        if (!isEmpty(src)) {
            return Integer.parseInt(src.trim());
        }
        return 0;
    }
    
    public static int toInt(final Object src) {
        if (!isEmpty(src)) {
            return Integer.parseInt(src.toString().trim());
        }
        return 0;
    }
    
    public static long toLong(final String src) {
        if (!isEmpty(src)) {
            return Long.parseLong(src.trim());
        }
        return 0L;
    }
    
    public static long toLong(final Object src) {
        if (!isEmpty(src)) {
            return toLong(src.toString());
        }
        return 0L;
    }
    
    public static double toDouble(final Object src) {
        if (!isEmpty(src)) {
            return toDouble(src.toString());
        }
        return 0.0;
    }
    
    public static double toDouble(final String src) {
        if (!isEmpty(src)) {
            return Double.parseDouble(src.trim());
        }
        return 0.0;
    }
    
    public static boolean toBoolean(final String src) {
        return !isEmpty(src) && Boolean.parseBoolean(src);
    }
    
    public static boolean toBoolean(final Object src) {
        return !isEmpty(src) && toBoolean(src.toString());
    }
    
    public static float toFloat(final String src) {
        if (!isEmpty(src)) {
            return Float.parseFloat(src);
        }
        return 0.0f;
    }
    
    public static float toFloat(final Object src) {
        if (!isEmpty(src)) {
            return toFloat(src.toString());
        }
        return 0.0f;
    }
    
    public static short toShort(final String src) {
        if (!isEmpty(src)) {
            return Short.parseShort(src);
        }
        return 0;
    }
    
    public static short toShort(final Object src) {
        if (!isEmpty(src)) {
            return toShort(src.toString());
        }
        return 0;
    }
    
    public static byte toByte(final String src) {
        if (!isEmpty(src)) {
            return Byte.parseByte(src);
        }
        return 0;
    }
    
    public static byte toByte(final Object src) {
        if (!isEmpty(src)) {
            return toByte(src.toString());
        }
        return 0;
    }
    
    public static char toChar(final String src) {
        if (!isEmpty(src)) {
            return src.trim().charAt(0);
        }
        return ' ';
    }
    
    public static char toChar(final Object src) {
        if (!isEmpty(src)) {
            return toChar(src.toString());
        }
        return ' ';
    }
    
    public static int trimLength(final String src) {
        if (src != null) {
            return src.trim().length();
        }
        return 0;
    }
    
    public static int length(final String src) {
        return src.length();
    }
    
    public static byte[] toBytes(final String s) {
        try {
            return s.getBytes("UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            StringUtil.LOG.error("UTF-8 not supported?", (Throwable)e);
            return null;
        }
    }
    
    public static boolean equals(final String src, final String dest) {
        return src == dest || (src != null && src.equals(dest));
    }
    
    public static int compare(final String src, final String dest) {
        if (src == dest) {
            return 0;
        }
        if (src == null) {
            return -1;
        }
        if (dest == null) {
            return 1;
        }
        return src.compareTo(dest);
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)StringUtil.class);
    }
}
