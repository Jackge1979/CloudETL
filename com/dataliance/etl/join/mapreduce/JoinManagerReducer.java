package com.dataliance.etl.join.mapreduce;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.*;

public class JoinManagerReducer extends Reducer<Text, Text, Text, Text>
{
    protected void reduce(final Text key, final Iterable<Text> values, final Reducer.Context context) throws IOException, InterruptedException {
    }
}
