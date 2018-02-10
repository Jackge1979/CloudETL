package com.dataliance.hadoop.job.mapreduce;

import org.apache.hadoop.mapreduce.*;

import com.dataliance.hadoop.vo.*;

import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;

public class TextSortJobReducer extends Reducer<DescLongWritable, Text, Text, DescLongWritable>
{
    protected void reduce(final DescLongWritable key, final Iterable<Text> values, final Reducer.Context context) throws IOException, InterruptedException {
        for (final Text value : values) {
            context.write((Object)value, (Object)key);
        }
    }
}
