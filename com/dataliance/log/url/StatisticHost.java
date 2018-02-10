package com.dataliance.log.url;

import com.dataliance.service.util.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import com.dataliance.log.url.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;

import com.dataliance.hadoop.*;
import com.dataliance.hadoop.job.*;

import org.apache.hadoop.fs.*;

public class StatisticHost extends AbstractTool
{
    public static void main(final String[] args) throws Exception {
        final Configuration conf = ConfigUtils.getConfig();
        ToolRunner.run(conf, (Tool)new StatisticHost(), args);
    }
    
    @Override
    protected void doAction(final Path[] in, final Path out) throws Exception {
        final Path tmpOut = new Path("tmp-" + System.currentTimeMillis());
        this.addInputPath(in);
        this.setOutputPath(tmpOut);
        this.setMapperClass(ExtractHostMap.class);
        this.setReducerClass(ExtractHostReduce.class);
        this.setOutputKeyClass(Text.class);
        this.setOutputValueClass(LongWritable.class);
        this.setOutputFormatClass((Class<? extends OutputFormat>)SequenceFileOutputFormat.class);
        this.setNumReduceTasks(20);
        this.runJob(true);
        System.out.println("SUCCESS extractURL.....\nWill do sort....");
        final SequenceFileSortJob sortTool = new SequenceFileSortJob();
        sortTool.initJob();
        sortTool.doAction(new Path[] { tmpOut }, out);
        System.out.println("Will delete tmp path " + tmpOut);
        final FileSystem fs = FileSystem.get(this.getConf());
        fs.delete(tmpOut, true);
    }
}
