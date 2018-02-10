package com.dataliance.json.util;

import net.sf.json.*;
import java.lang.reflect.*;
import java.util.*;

import com.dataliance.json.filter.*;

public class JsonUtil
{
    public static final String METHOD_GET = "get";
    public static final String METHOD_IS = "is";
    
    public static JSONObject toJson(final Object obj, final List<Filter> filters) throws Exception {
        final JSONObject json = new JSONObject();
        final Method[] arr$;
        final Method[] methods = arr$ = obj.getClass().getMethods();
        for (final Method method : arr$) {
            final String name = method.getName();
            if ((name.startsWith("get") || name.startsWith("is")) && !name.equals("getClass")) {
                String field = toAttribute(name, "get");
                if (name.startsWith("is")) {
                    field = toAttribute(name, "is");
                }
                if (filters != null && !filters.isEmpty()) {
                    for (final Filter f : filters) {
                        field = f.filter(field);
                        if (field == null) {
                            break;
                        }
                    }
                }
                if (field != null) {
                    json.accumulate(field, method.invoke(obj, (Object[])null));
                }
            }
        }
        return json;
    }
    
    public static JSONObject toJson(final Object obj) throws Exception {
        return toJson(obj, null);
    }
    
    private static String toAttribute(String method, final String type) {
        method = method.replaceAll("^" + type, "");
        final char v = method.charAt(0);
        return method.replaceAll("^" + v, String.valueOf(Character.toLowerCase(v)));
    }
}
