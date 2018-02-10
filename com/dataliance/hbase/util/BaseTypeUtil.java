package com.dataliance.hbase.util;

import org.apache.hadoop.hbase.util.*;
import java.io.*;
import java.util.*;

public class BaseTypeUtil
{
    protected static final Map<Class<?>, Type> BASETYPEMAPING;
    
    public static boolean isBaseType(final Class<?> clazz) {
        return BaseTypeUtil.BASETYPEMAPING.keySet().contains(clazz);
    }
    
    protected static Type getType(final Class<?> clazz) {
        if (isBaseType(clazz)) {
            return BaseTypeUtil.BASETYPEMAPING.get(clazz);
        }
        return Type.UNW;
    }
    
    private static Type getType(final String className) {
        try {
            return getType(Class.forName(className));
        }
        catch (ClassNotFoundException e) {
            return Type.UNW;
        }
    }
    
    public static Object getValue(final String className, final byte[] value) {
        final Type type = getType(className);
        return getValue(type, value);
    }
    
    public static Object getValue(final Class<?> clazz, final byte[] value) {
        return getValue(getType(clazz), value);
    }
    
    private static Object getValue(final Type type, final byte[] value) {
        try {
            switch (type) {
                case STRING: {
                    return Bytes.toString(value);
                }
                case INT: {
                    return Bytes.toInt(value);
                }
                case FLOAT: {
                    return Bytes.toFloat(value);
                }
                case LONG: {
                    return Bytes.toLong(value);
                }
                case DOUBLE: {
                    return Bytes.toDouble(value);
                }
                case SHORT: {
                    return Bytes.toShort(value);
                }
                case BOOLEAN: {
                    return Bytes.toBoolean(value);
                }
                default: {
                    return null;
                }
            }
        }
        catch (Exception e) {
            return null;
        }
    }
    
    public static byte[] toBytes(final Object value) throws IOException {
        final Type type = getType(value.getClass());
        try {
            switch (type) {
                case STRING: {
                    return Bytes.toBytes((String)value);
                }
                case INT: {
                    return Bytes.toBytes((int)value);
                }
                case FLOAT: {
                    return Bytes.toBytes((float)value);
                }
                case LONG: {
                    return Bytes.toBytes((long)value);
                }
                case DOUBLE: {
                    return Bytes.toBytes((double)value);
                }
                case SHORT: {
                    return Bytes.toBytes((short)value);
                }
                case BOOLEAN: {
                    return Bytes.toBytes((boolean)value);
                }
                case BYTES: {
                    return (byte[])value;
                }
                default: {
                    return null;
                }
            }
        }
        catch (Exception e) {
            throw new IOException(value.getClass() + " Unknow class");
        }
    }
    
    static {
        (BASETYPEMAPING = new HashMap<Class<?>, Type>()).put(Integer.TYPE, Type.INT);
        BaseTypeUtil.BASETYPEMAPING.put(Float.TYPE, Type.FLOAT);
        BaseTypeUtil.BASETYPEMAPING.put(Long.TYPE, Type.LONG);
        BaseTypeUtil.BASETYPEMAPING.put(Byte.TYPE, Type.BYTE);
        BaseTypeUtil.BASETYPEMAPING.put(Double.TYPE, Type.DOUBLE);
        BaseTypeUtil.BASETYPEMAPING.put(Boolean.TYPE, Type.BOOLEAN);
        BaseTypeUtil.BASETYPEMAPING.put(Short.TYPE, Type.SHORT);
        BaseTypeUtil.BASETYPEMAPING.put(Character.TYPE, Type.CHAR);
        BaseTypeUtil.BASETYPEMAPING.put(String.class, Type.STRING);
        BaseTypeUtil.BASETYPEMAPING.put(Date.class, Type.DATE);
        BaseTypeUtil.BASETYPEMAPING.put(Integer.class, Type.INT);
        BaseTypeUtil.BASETYPEMAPING.put(Float.class, Type.FLOAT);
        BaseTypeUtil.BASETYPEMAPING.put(Long.class, Type.LONG);
        BaseTypeUtil.BASETYPEMAPING.put(Byte.class, Type.BYTE);
        BaseTypeUtil.BASETYPEMAPING.put(Double.class, Type.DOUBLE);
        BaseTypeUtil.BASETYPEMAPING.put(Boolean.class, Type.BOOLEAN);
        BaseTypeUtil.BASETYPEMAPING.put(Short.class, Type.SHORT);
        BaseTypeUtil.BASETYPEMAPING.put(Character.class, Type.CHAR);
        BaseTypeUtil.BASETYPEMAPING.put(byte[].class, Type.BYTES);
    }
    
    protected enum Type
    {
        INT, 
        FLOAT, 
        LONG, 
        BYTE, 
        DOUBLE, 
        BOOLEAN, 
        SHORT, 
        CHAR, 
        STRING, 
        DATE, 
        UNW, 
        BYTES;
    }
}
