package com.dataliance.analysis.data.mapreduce;

import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.commons.cli.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapreduce.*;
import java.util.*;

public class UserFrequencyStatistics extends Configured implements Tool
{
    private static final Log LOG;
    public static final String OUTPUT_NAME = "frequency";
    
    public int run(final String[] args) throws Exception {
        int exitCode = -1;
        final Options options = buildOptions();
        CommandLine commands = null;
        try {
            final BasicParser parser = new BasicParser();
            commands = parser.parse(options, args);
        }
        catch (ParseException e) {
            printUsage(options);
            return exitCode;
        }
        if (!commands.hasOption("input")) {
            printUsage(options);
            return -1;
        }
        if (!commands.hasOption("output")) {
            printUsage(options);
            return -1;
        }
        final long startTime = System.currentTimeMillis();
        long endTime = 0L;
        double elapsedTime = 0.0;
        final Job job = this.createSubmittableJob(commands);
        exitCode = (job.waitForCompletion(true) ? 0 : 1);
        endTime = System.currentTimeMillis();
        elapsedTime = (endTime - startTime) / 1000.0;
        UserFrequencyStatistics.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        UserFrequencyStatistics.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public Job createSubmittableJob(final CommandLine commands) throws IOException {
        final Configuration conf = this.getConf();
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final Path outputPath = new Path(commands.getOptionValue("output"), "frequency");
        final FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        final Job job = new Job(conf);
        job.setJobName("UserFrequencyStatistics");
        job.setJarByClass((Class)UserFrequencyStatistics.class);
        FileInputFormat.setInputPaths(job, commaSeparatedPaths);
        FileOutputFormat.setOutputPath(job, outputPath);
        UserFrequencyStatistics.LOG.info((Object)("commaSeparatedPaths:" + commaSeparatedPaths));
        job.setInputFormatClass((Class)TextInputFormat.class);
        job.setMapperClass((Class)PhoneNumberMapper.class);
        job.setMapOutputKeyClass((Class)Text.class);
        job.setMapOutputValueClass((Class)LongWritable.class);
        job.setReducerClass((Class)PhoneNumberReducer.class);
        job.setOutputKeyClass((Class)LongWritable.class);
        job.setOutputValueClass((Class)Text.class);
        job.setOutputFormatClass((Class)SequenceFileOutputFormat.class);
        return job;
    }
    
    private static final Options buildOptions() {
        final Options options = new Options();
        options.addOption("input", true, "[must] input directory of data");
        options.addOption("output", true, "[must] target directory of data");
        return options;
    }
    
    private static final void printUsage(final Options options) {
        final HelpFormatter help = new HelpFormatter();
        help.printHelp("UserFrequencyStatistics", options);
    }
    
    static {
        LOG = LogFactory.getLog((Class)UserFrequencyStatistics.class);
    }
    
    public static class PhoneNumberMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        Text outKey;
        LongWritable count;
        
        public PhoneNumberMapper() {
            this.outKey = new Text();
            this.count = new LongWritable();
        }
        
        public void setup(final Mapper.Context context) {
        }
        
        public void map(final LongWritable key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
            if ("".equals(value.toString().trim())) {
                return;
            }
            final String[] result = value.toString().split("\t");
            if (result.length != 2) {
                return;
            }
            final String phoneNumber = result[1];
            this.outKey.set(phoneNumber);
            this.count.set(1L);
            context.write((Object)this.outKey, (Object)this.count);
        }
    }
    
    public static class PhoneNumberReducer extends Reducer<Text, LongWritable, LongWritable, Text>
    {
        private LongWritable result;
        
        public PhoneNumberReducer() {
            this.result = null;
        }
        
        protected void setup(final Reducer.Context context) throws IOException, InterruptedException {
            this.result = new LongWritable();
        }
        
        protected void reduce(final Text key, final Iterable<LongWritable> values, final Reducer.Context context) throws IOException, InterruptedException {
            long totalCnt = 0L;
            for (final LongWritable value : values) {
                totalCnt += value.get();
            }
            this.result.set(totalCnt);
            context.write((Object)this.result, (Object)key);
        }
    }
}
