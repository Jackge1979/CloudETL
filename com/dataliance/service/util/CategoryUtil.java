package com.dataliance.service.util;

import com.dataliance.util.*;

public class CategoryUtil
{
    private static final int MAX_CATEGORY_LEN = 5;
    
    public static String fillCategory(final String category) {
        return NumFormat.prefixFill(category, 5, "0");
    }
    
    public static String parseCategory(final String category) {
        return category.replaceFirst("^0+", "");
    }
    
    public static String createRowKey(final String category, final String url) {
        return fillCategory(category) + "|" + url;
    }
}
