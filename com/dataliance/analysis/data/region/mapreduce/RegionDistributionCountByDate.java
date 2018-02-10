package com.dataliance.analysis.data.region.mapreduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

public class RegionDistributionCountByDate extends Configured implements Tool
{
    private static final Log LOG;
    
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
        final JobConf job = this.createSubmittableJob(commands);
        final RunningJob runningJob = JobClient.runJob(job);
        exitCode = runningJob.getJobState();
        endTime = System.currentTimeMillis();
        elapsedTime = (endTime - startTime) / 1000.0;
        RegionDistributionCountByDate.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        RegionDistributionCountByDate.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public JobConf createSubmittableJob(final CommandLine commands) throws IOException {
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final String outputPath = commands.getOptionValue("output");
        final JobConf job = new JobConf(this.getConf());
        job.setJobName("RegionDistribution Count");
        job.setJarByClass((Class)RegionDistributionCountByDate.class);
        job.setMapperClass((Class)MapClass.class);
        job.setCombinerClass((Class)ReduceClass.class);
        job.setReducerClass((Class)ReduceClass.class);
        job.setPartitionerClass((Class)FirstPartitioner.class);
        job.setMapOutputKeyClass((Class)Text.class);
        job.setMapOutputValueClass((Class)IntWritable.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)IntWritable.class);
        job.setSpeculativeExecution(false);
        FileInputFormat.setInputPaths(job, commaSeparatedPaths);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
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
        help.printHelp("RegionDistribution", options);
    }
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        ToolRunner.run(conf, (Tool)new RegionDistributionCountByDate(), args);
    }
    
    static {
        LOG = LogFactory.getLog((Class)RegionDistributionCountByDate.class);
    }
    
    public static class FirstPartitioner implements Partitioner<Text, IntWritable>
    {
        public int getPartition(final Text key, final IntWritable value, final int numPatitions) {
            return Math.abs(key.hashCode() * 127) % numPatitions;
        }
        
        public void configure(final JobConf arg0) {
        }
    }
    
    public static class MapClass extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable>
    {
        public void map(final Object object, final Text value, final OutputCollector<Text, IntWritable> output, final Reporter reporter) throws IOException {
            final String str = value.toString();
            final String[] fieldValues = str.split("\\s+");
            if (fieldValues.length != 2) {
                return;
            }
            final String area = fieldValues[0];
            output.collect((Object)new Text(area), (Object)new IntWritable(1));
        }
    }
    
    public static class ReduceClass extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
    {
        public IntWritable total;
        
        public ReduceClass() {
            this.total = new IntWritable(0);
        }
        
        public void reduce(final Text key, final Iterator<IntWritable> values, final OutputCollector<Text, IntWritable> output, final Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            this.total.set(sum);
            output.collect((Object)key, (Object)this.total);
        }
    }
}
