package com.dataliance.hadoop.job.mapreduce;

import org.apache.hadoop.mapreduce.*;

import com.dataliance.hadoop.vo.*;

import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;

public class SequenceFileSortJobReducer extends Reducer<DescLongWritable, Text, Text, LongWritable>
{
    protected void reduce(final DescLongWritable key, final Iterable<Text> iterable, final Reducer.Context context) throws IOException, InterruptedException {
        for (final Text value : iterable) {
            context.write((Object)value, (Object)key);
        }
    }
}
