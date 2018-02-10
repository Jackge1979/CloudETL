package com.dataliance.etl;

import com.dataliance.service.util.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;

import com.dataliance.etl.inject.rpc.impl.*;
import com.dataliance.etl.workflow.process.*;
import com.dataliance.hadoop.mapreduce.output.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.*;
import java.util.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;

public class MontiorTool extends Configured implements Tool
{
    public static void main(final String[] args) throws Exception {
        final Configuration conf = ConfigUtils.getConfig();
        ToolRunner.run(conf, (Tool)new MontiorTool(), args);
    }
    
    public int run(final String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("arg count : " + args.length);
            System.out.println(String.format("Usage : %s  {'name': 'value'}", ImportManager.class.getSimpleName()));
            System.exit(1);
        }
        Map<String, String> jobInfo = new HashMap<String, String>();
        try {
            jobInfo = ParameterParser.convertJson2Map(args[0]);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.print("arg: " + args[0]);
            System.out.println(String.format("Usage : %s  {'name': 'value'}", ImportManager.class.getSimpleName()));
            System.exit(1);
        }
        final String id = jobInfo.get(ETLConstants.PROGRAM_ID.PD_id.toString());
        final String inDir = jobInfo.get(ETLConstants.INPUT_DATA.ID_from.toString());
        final String outDir = jobInfo.get(ETLConstants.OUTPUT_DATA.OD_target.toString());
        final Path in = new Path(inDir);
        final Path out = new Path(outDir);
        final Job job = new Job(this.getConf());
        job.setJobName("TEST-JOB-" + id);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setOutputFormatClass((Class)LineOutputFormat.class);
        job.setOutputKeyClass((Class)LongWritable.class);
        job.setOutputValueClass((Class)Text.class);
        job.setJarByClass((Class)MontiorTool.class);
        job.setMapperClass((Class)MontiorMapper.class);
        return 0;
    }
    
    public static class MontiorMapper extends Mapper<LongWritable, Text, LongWritable, Text>
    {
        protected void map(final LongWritable key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
            context.write((Object)key, (Object)value);
        }
    }
}
