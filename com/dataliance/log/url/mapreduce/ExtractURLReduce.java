package com.dataliance.log.url.mapreduce;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;

public class ExtractURLReduce extends Reducer<Text, LongWritable, Text, LongWritable>
{
    private LongWritable lastValue;
    
    public ExtractURLReduce() {
        this.lastValue = new LongWritable(0L);
    }
    
    protected void reduce(final Text key, final Iterable<LongWritable> iterable, final Reducer.Context context) throws IOException, InterruptedException {
        this.lastValue.set(0L);
        for (final LongWritable value : iterable) {
            this.lastValue.set(value.get() + this.lastValue.get());
        }
        context.write((Object)key, (Object)this.lastValue);
    }
}
