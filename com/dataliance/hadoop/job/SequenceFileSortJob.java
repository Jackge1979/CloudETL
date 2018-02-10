package com.dataliance.hadoop.job;

import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;

import com.dataliance.hadoop.*;
import com.dataliance.hadoop.job.mapreduce.*;
import com.dataliance.hadoop.mapreduce.output.*;
import com.dataliance.hadoop.vo.*;
import com.dataliance.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class SequenceFileSortJob extends AbstractTool
{
    public static void main(final String[] args) throws Exception {
        final Configuration conf = DAConfigUtil.create();
        ToolRunner.run(conf, (Tool)new SequenceFileSortJob(), args);
    }
    
    public void doAction(final Path[] in, final Path out) throws Exception {
        this.addInputPath(in);
        this.setOutputPath(out);
        this.setInputFormatClass((Class<? extends InputFormat>)SequenceFileInputFormat.class);
        this.setMapperClass(SequenceFileSortJobMapper.class);
        this.setReducerClass(SequenceFileSortJobReducer.class);
        this.setOutputKeyClass(DescLongWritable.class);
        this.setOutputValueClass(Text.class);
        this.setOutputFormatClass((Class<? extends OutputFormat>)VKLineOutputFormat.class);
        this.runJob(true);
    }
}
