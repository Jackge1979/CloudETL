package com.dataliance.analysis.data.url.mapreduce;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import java.io.*;
import org.apache.hadoop.conf.*;

public class InjectURLMapper extends Mapper<WritableComparable<?>, Text, ImmutableBytesWritable, Put>
{
    private ImmutableBytesWritable lastKey;
    private byte[] family;
    private byte[] qualifier;
    private byte[] NULLVALUE;
    
    public InjectURLMapper() {
        this.lastKey = new ImmutableBytesWritable();
        this.NULLVALUE = new byte[0];
    }
    
    protected void map(final WritableComparable<?> key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
        final byte[] rowKey = Bytes.toBytes("0|" + value);
        this.lastKey.set(rowKey);
        final Put put = new Put(rowKey);
        put.add(this.family, this.qualifier, this.NULLVALUE);
        context.write((Object)this.lastKey, (Object)put);
    }
    
    protected void setup(final Mapper.Context context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        this.family = Bytes.toBytes(conf.get("hbase.table.family"));
        this.qualifier = Bytes.toBytes(conf.get("hbase.table.qualifier"));
    }
}
