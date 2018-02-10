package com.dataliance.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.*;
import java.util.*;
import java.io.*;

public class BasicReducer<KEY, VALUE> extends Reducer<KEY, VALUE, KEY, VALUE>
{
    protected void reduce(final KEY key, final Iterable<VALUE> values, final Reducer.Context context) throws IOException, InterruptedException {
        for (final VALUE value : values) {
            context.write((Object)key, (Object)value);
        }
    }
}
