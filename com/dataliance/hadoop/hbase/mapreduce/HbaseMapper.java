package com.dataliance.hadoop.hbase.mapreduce;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.client.*;
import java.io.*;

public class HbaseMapper<KEYOUT, VALUEOUT> extends Mapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT>
{
    protected void map(final ImmutableBytesWritable key, final Result value, final Mapper.Context context) throws IOException, InterruptedException {
    }
}
