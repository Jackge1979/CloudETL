package com.dataliance.log.url;

import com.dataliance.service.util.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import com.dataliance.log.url.mapreduce.*;
import org.apache.hadoop.io.*;

import com.dataliance.hadoop.*;
import com.dataliance.hadoop.mapreduce.output.*;

import org.apache.hadoop.mapreduce.*;

public class ExtractURL extends AbstractTool
{
    public static void main(final String[] args) throws Exception {
        final Configuration conf = ConfigUtils.getConfig();
        ToolRunner.run(conf, (Tool)new ExtractURL(), args);
    }
    
    @Override
    protected void doAction(final Path[] in, final Path out) throws Exception {
        this.addInputPath(in);
        this.setOutputPath(out);
        this.setMapperClass(ExtractURLMap.class);
        this.setReducerClass(ExtractURLReduce.class);
        this.setOutputKeyClass(Text.class);
        this.setOutputValueClass(LongWritable.class);
        this.setOutputFormatClass((Class<? extends OutputFormat>)KeyLineOutputFormat.class);
        this.runJob(true);
    }
}
