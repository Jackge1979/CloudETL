package com.dataliance.hadoop.util;

import org.apache.hadoop.io.*;

import com.dataliance.hbase.util.*;

import java.io.*;

public class BaseTypeWrite extends BaseTypeUtil
{
    public static void write(final DataOutput out, final Object value) throws IOException {
        final Type type = BaseTypeUtil.getType(value.getClass());
        switch (type) {
            case STRING: {
                WritableUtils.writeString(out, (String)value);
                break;
            }
            case INT: {
                out.writeInt((int)value);
                break;
            }
            case FLOAT: {
                out.writeFloat((float)value);
                break;
            }
            case LONG: {
                out.writeLong((long)value);
                break;
            }
            case DOUBLE: {
                out.writeDouble((double)value);
                break;
            }
            case SHORT: {
                out.writeShort((short)value);
                break;
            }
            case BOOLEAN: {
                out.writeBoolean((boolean)value);
                break;
            }
        }
    }
}
