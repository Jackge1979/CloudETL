package com.dataliance.analysis.data.mapreduce.driver;

import org.apache.hadoop.conf.*;
import com.dataliance.hbase.data.mapreduce.*;
import org.apache.hadoop.util.*;

public class TopTenDriver
{
    public static void main(String[] args) throws Exception {
        args = new String[] { "-m", "5", "-r", "1", "-inFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat", "-outFormat", "org.apache.hadoop.mapred.TextOutputFormat", "-outKey", "org.apache.hadoop.io.LongWritable", "-outValue", "org.apache.hadoop.io.Text", "-totalOrder", "0.2", "20", "6", "E:\\git-repository\\git\\bigdata-core\\statistic1332143377587", "./sort" + System.currentTimeMillis() };
        final Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        ToolRunner.run(conf, (Tool)new Sort(), args);
    }
}
