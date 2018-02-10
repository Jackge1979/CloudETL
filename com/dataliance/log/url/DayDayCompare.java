package com.dataliance.log.url;

import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import com.dataliance.log.url.mapreduce.*;
import org.apache.hadoop.io.*;

import com.dataliance.hadoop.*;
import com.dataliance.hadoop.mapreduce.output.*;
import com.dataliance.util.*;

import org.apache.hadoop.mapreduce.*;

public class DayDayCompare extends AbstractTool
{
    public static void main(final String[] args) throws Exception {
        final Configuration conf = DAConfigUtil.create();
        ToolRunner.run(conf, (Tool)new DayDayCompare(), args);
    }
    
    @Override
    protected void doAction(final Path[] in, final Path out) throws Exception {
        this.addInputPath(in);
        this.setOutputPath(out);
        this.setMapperClass(DayDayCompareMapper.class);
        this.setReducerClass(DayDayCompareReducer.class);
        this.setOutputKeyClass(Text.class);
        this.setOutputValueClass(LongWritable.class);
        this.setOutputFormatClass((Class<? extends OutputFormat>)KVLineOutputFormat.class);
        this.runJob(true);
    }
}
