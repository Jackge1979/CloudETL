package com.dataliance.util;

public class NumFormat
{
    public static final String ZERO = "0";
    public static final String NINE = "9";
    public static final int DEF_SIZE = 20;
    public static final String DEFAULT_MAX = "99999999999999999999";
    public static final String FILL_WELL = "#";
    public static final String FILL_VERTICAL_LINE = "|";
    public static final int DEF_HOST_SIZE = 200;
    
    public static final String suffixFill(String extKey, final int size, final String fill) {
        extKey = ((extKey == null) ? "" : extKey);
        final StringBuffer sb = new StringBuffer();
        sb.append(extKey);
        int i;
        for (int len = i = extKey.length(); i < size; ++i) {
            sb.append(fill);
        }
        return sb.toString();
    }
    
    public static final String suffixByWellFill(final String extKey, final int size) {
        return suffixFill(extKey, size, "#");
    }
    
    public static final String suffixByVerticalLineFill(final String extKey, final int size) {
        return suffixFill(extKey, size, "|");
    }
    
    public static final String suffixByWellFill(final String extKey) {
        return suffixFill(extKey, 200, "#");
    }
    
    public static final String suffixByVerticalLineFill(final String extKey) {
        return suffixFill(extKey, 200, "|");
    }
    
    public static final String reductionByWellFill(final String dest) {
        return dest.replaceAll("(#+)$", "");
    }
    
    public static final String reductionByVerticalLineFill(final String dest) {
        return dest.replaceAll("(\\|+)$", "");
    }
    
    public static final String prefixFill(String extKey, final int size, final String fill) {
        extKey = ((extKey == null) ? "" : extKey);
        final int len = extKey.length();
        final StringBuffer sb = new StringBuffer();
        for (int i = len; i < size; ++i) {
            sb.append(fill);
        }
        sb.append(extKey);
        return sb.toString();
    }
    
    public static final String prefixByWellFill(final String extKey, final int size) {
        return prefixFill(extKey, size, "#");
    }
    
    public static final String prefixByVerticalLineFill(final String extKey, final int size) {
        return prefixFill(extKey, size, "|");
    }
    
    public static final String prefixFill(final long extKey, final int size, final String fill) {
        final String l = Long.toString(extKey);
        final int len = l.length();
        final StringBuffer sb = new StringBuffer();
        for (int i = len; i < size; ++i) {
            sb.append(fill);
        }
        sb.append(l);
        return sb.toString();
    }
    
    public static final String prefixByWellFill(final long extKey, final int size) {
        return prefixFill(extKey, size, "#");
    }
    
    public static final String suffixByZero(final long extKey, final int size) {
        return suffixFill(extKey, size, "0");
    }
    
    public static final String suffixFill(final long extKey, final int size, final String fill) {
        final String l = Long.toString(extKey);
        final int len = l.length();
        final StringBuffer sb = new StringBuffer();
        for (int i = len; i < size; ++i) {
            sb.append(fill);
        }
        sb.append(l);
        return sb.toString();
    }
    
    public static final String prefixByVerticalLineFill(final long extKey, final int size) {
        return prefixFill(extKey, size, "|");
    }
    
    public static final String prefixByZero(final long extKey, final int size) {
        return prefixFill(extKey, size, "0");
    }
    
    public static final String formatByZero(final long extKey) {
        return prefixFill(extKey, 20, "0");
    }
    
    public static final String formatByNine(final long extKey, final int size) {
        return prefixFill(extKey, size, "9");
    }
    
    public static final String formatByNine(final long extKey) {
        return prefixFill(extKey, 20, "9");
    }
    
    public static final String getDefault() {
        return "99999999999999999999";
    }
}
