package com.dataliance.hadoop.vo;

import org.apache.hadoop.io.*;

public class DescLongWritable extends LongWritable
{
    public DescLongWritable() {
    }
    
    public DescLongWritable(final LongWritable value) {
        this.set(value.get());
    }
    
    public DescLongWritable(final long value) {
        this.set(value);
    }
    
    public int compareTo(final LongWritable o) {
        final long thisValue = this.get();
        final long thatValue = o.get();
        return (thisValue == thatValue) ? 0 : ((thatValue < thisValue) ? -1 : 1);
    }
}
