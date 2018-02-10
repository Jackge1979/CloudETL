package com.dataliance.etl.join.mapreduce;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.*;

public class JoinManagerMapper extends Mapper<LongWritable, Text, Text, Text>
{
    protected void map(final LongWritable key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
    }
    
    protected void setup(final Mapper.Context context) throws IOException, InterruptedException {
    }
}
