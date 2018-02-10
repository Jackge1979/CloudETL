package com.dataliance.log.url.mapreduce;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.*;

public class DayDayCompareMapper extends Mapper<WritableComparable<?>, Text, Text, LongWritable>
{
    private LongWritable COUNT;
    
    public DayDayCompareMapper() {
        this.COUNT = new LongWritable(1L);
    }
    
    protected void map(final WritableComparable<?> key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
        context.write((Object)value, (Object)this.COUNT);
    }
}
