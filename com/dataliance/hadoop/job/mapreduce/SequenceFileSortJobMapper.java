package com.dataliance.hadoop.job.mapreduce;

import org.apache.hadoop.mapreduce.*;

import com.dataliance.hadoop.vo.*;

import org.apache.hadoop.io.*;

import java.io.*;

public class SequenceFileSortJobMapper extends Mapper<Text, LongWritable, DescLongWritable, Text>
{
    private DescLongWritable lastKey;
    
    public SequenceFileSortJobMapper() {
        this.lastKey = new DescLongWritable(0L);
    }
    
    protected void map(final Text key, final LongWritable value, final Mapper.Context context) throws IOException, InterruptedException {
        this.lastKey.set(value.get());
        context.write((Object)this.lastKey, (Object)key);
    }
}
