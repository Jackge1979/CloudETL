package com.dataliance.hadoop.vo;

import org.apache.hadoop.io.*;

import com.dataliance.hadoop.annotation.*;

import java.lang.annotation.*;
import java.lang.reflect.*;
import java.util.*;
import java.io.*;

public abstract class AbstractWritable implements Writable
{
    public static final String SET = "set";
    public static final String GET = "get";
    public static final String IS = "is";
    
    private List<MethodCompare> getMethodCompares() {
        final List<MethodCompare> values = new ArrayList<MethodCompare>();
        final Method[] arr$;
        final Method[] methods = arr$ = this.getClass().getMethods();
        for (final Method method : arr$) {
            final String name = method.getName();
            if ((!name.startsWith("get") && !name.startsWith("is")) || name.equals("getClass") || !method.isAnnotationPresent(NoWrite.class)) {
                final WriteOrderNum won = method.getAnnotation(WriteOrderNum.class);
                if (won != null) {
                    final int num = won.num();
                    try {
                        final Object value = method.invoke(this, (Object[])null);
                        values.add(new MethodCompare(num, value));
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        Collections.sort(values);
        return values;
    }
    
    public void write(final DataOutput out) throws IOException {
        final List<MethodCompare> values = this.getMethodCompares();
        for (MethodCompare value : values) {}
    }
    
    public void readFields(final DataInput in) throws IOException {
    }
}
