package com.dataliance.log.url;

import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import com.dataliance.hadoop.*;
import com.dataliance.hadoop.mapreduce.input.*;
import com.dataliance.util.*;

import org.apache.hadoop.io.*;
import com.dataliance.log.url.vo.*;
import com.dataliance.log.url.mapreduce.*;
import com.dataliance.log.url.mapreduce.output.*;
import org.apache.hadoop.mapreduce.*;

public class AllDayCompare extends AbstractTool
{
    public static void main(final String[] args) throws Exception {
        final Configuration conf = DAConfigUtil.create();
        ToolRunner.run(conf, (Tool)new AllDayCompare(), args);
    }
    
    @Override
    protected void doAction(final Path[] in, final Path out) throws Exception {
        this.addInputPath(in);
        this.setOutputPath(out);
        this.setInputFormatClass((Class<? extends InputFormat>)FileKeyInputFormat.class);
        this.setOutputKeyClass(Text.class);
        this.setOutputValueClass(AllDayVO.class);
        this.setMapperClass(AllDayCompareMapper.class);
        this.setReducerClass(AllDayCompareReducer.class);
        this.getJobConf().setInt("com.DA.intput.path.num", in.length);
        this.setOutputFormatClass((Class<? extends OutputFormat>)AllDayOutputFormat.class);
        this.runJob(true);
    }
}
