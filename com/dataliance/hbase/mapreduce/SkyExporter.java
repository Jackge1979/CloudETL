package com.dataliance.hbase.mapreduce;

import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;

public class DAExporter extends TableMapper<ImmutableBytesWritable, Result>
{
    public static final long DEF_ROW_NUM = -1L;
    public static final String DEF_ROW_NUM_KEY = "DA.hbase.export.rownum";
    private long rowNum;
    private long currentNum;
    
    public DAExporter() {
        this.currentNum = 0L;
    }
    
    public void map(final ImmutableBytesWritable row, final Result value, final Mapper.Context context) throws IOException {
        try {
            if (this.rowNum <= 0L || this.currentNum < this.rowNum) {
                context.write((Object)row, (Object)value);
            }
            ++this.currentNum;
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    protected void setup(final Mapper.Context context) throws IOException, InterruptedException {
        this.rowNum = context.getConfiguration().getLong("DA.hbase.export.rownum", -1L);
    }
}
