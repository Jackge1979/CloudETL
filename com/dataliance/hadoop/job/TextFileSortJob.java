package com.dataliance.hadoop.job;

import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import com.dataliance.hadoop.*;
import com.dataliance.hadoop.job.mapreduce.*;
import com.dataliance.hadoop.mapreduce.output.*;
import com.dataliance.hadoop.vo.*;
import com.dataliance.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class TextFileSortJob extends AbstractTool
{
    public static void main(final String[] args) throws Exception {
        final Configuration conf = DAConfigUtil.create();
        ToolRunner.run(conf, (Tool)new TextFileSortJob(), args);
    }
    
    public void doAction(final Path[] in, final Path out) throws Exception {
        this.addInputPath(in);
        this.setOutputPath(out);
        this.setMapperClass(TextSortJobMapper.class);
        this.setReducerClass(TextSortJobReducer.class);
        this.setOutputKeyClass(DescLongWritable.class);
        this.setOutputValueClass(Text.class);
        this.setOutputFormatClass((Class<? extends OutputFormat>)VKLineOutputFormat.class);
        this.runJob(true);
    }
}
