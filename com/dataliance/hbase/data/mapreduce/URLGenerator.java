package com.dataliance.hbase.data.mapreduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.*;
import java.util.*;
import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;

public class URLGenerator extends Configured implements Tool
{
    private static final String NAME = "URLGenerator";
    private static final String COLUMN_INDEX_KEY = "column.index";
    
    public int run(final String[] args) throws Exception {
        final Options options = buildOptions();
        final BasicParser parser = new BasicParser();
        final CommandLine commands = parser.parse(options, args);
        if (!commands.hasOption("inputpath")) {
            printUsage(options);
            return -1;
        }
        final String outputPath = "URLGenerator_" + System.currentTimeMillis();
        if (!commands.hasOption("outputpath")) {}
        final String inputPath = commands.getOptionValue("inputpath");
        System.err.println("inputpath:" + inputPath);
        final Job job = new Job(this.getConf());
        job.setJobName("URLGenerator");
        job.setJarByClass((Class)URLGenerator.class);
        job.setMapperClass((Class)URLMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)LongWritable.class);
        job.setInputFormatClass((Class)TextInputFormat.class);
        job.setOutputFormatClass((Class)TextOutputFormat.class);
        final Path inputDir = new Path(inputPath);
        final Path outputDir = new Path(outputPath);
        FileInputFormat.setInputPaths(job, new Path[] { inputDir });
        FileOutputFormat.setOutputPath(job, outputDir);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    private static final Options buildOptions() {
        final Options options = new Options();
        options.addOption("inputpath", true, "[must] input data of src");
        options.addOption("outputpath", true, "[option] output data to dest default /user/demo/");
        return options;
    }
    
    private static final void printUsage(final Options options) {
        final HelpFormatter help = new HelpFormatter();
        help.printHelp("URLGenerator", options);
    }
    
    public static void main(final String[] args) throws Exception {
        final int exitCode = ToolRunner.run((Tool)new URLGenerator(), args);
        System.exit(exitCode);
    }
    
    static class URLReducer extends Reducer<Text, LongWritable, Text, LongWritable>
    {
        protected void reduce(final Text key, final Iterable<LongWritable> values, final Reducer.Context context) throws IOException, InterruptedException {
            for (final LongWritable phoneNumber : values) {
                context.write((Object)key, (Object)phoneNumber);
            }
        }
    }
    
    static class URLMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        private int coloumnIndex;
        
        URLMapper() {
            this.coloumnIndex = 0;
        }
        
        protected void setup(final Mapper.Context context) throws IOException, InterruptedException {
            super.setup(context);
            final Configuration conf = context.getConfiguration();
            this.coloumnIndex = conf.getInt("column.index", 12);
        }
        
        protected void map(final LongWritable key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
            final String[] values = value.toString().split(",");
            long phoneNumber = -1L;
            final int length = values.length;
            String url = null;
            try {
                if (3 < length) {
                    phoneNumber = Long.parseLong(values[2].replace("'", ""));
                }
                if (this.coloumnIndex < length) {
                    url = values[this.coloumnIndex - 1].replace("'", "");
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                phoneNumber = -1L;
            }
            System.out.println(String.format("mapper phoneNumber:%s url:%s", phoneNumber, url));
            context.write((Object)new Text(url), (Object)new LongWritable(phoneNumber));
        }
    }
}
