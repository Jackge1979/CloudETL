package com.dataliance.analysis.data.mapreduce.common;

import org.apache.hadoop.io.*;

public class DecreasingLongWritable extends LongWritable
{
    public DecreasingLongWritable() {
    }
    
    public DecreasingLongWritable(final long value) {
        super(value);
    }
    
    public void set(final long value) {
        super.set(value);
    }
    
    public int compareTo(final LongWritable o) {
        final long thisValue = super.get();
        final long thatValue = ((DecreasingLongWritable)o).get();
        return (thisValue < thatValue) ? 1 : ((thisValue == thatValue) ? 0 : -1);
    }
    
    static {
        WritableComparator.define((Class)DecreasingLongWritable.class, (WritableComparator)new LongWritable.DecreasingComparator());
    }
}
