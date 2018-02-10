package com.dataliance.analysis.data.flow.mapreduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

public class FlowCountByDate extends Configured implements Tool
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
        FlowCountByDate.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        FlowCountByDate.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public JobConf createSubmittableJob(final CommandLine commands) throws IOException {
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final String outputPath = commands.getOptionValue("output");
        final JobConf job = new JobConf(this.getConf());
        job.setJobName("FlowCount");
        job.setJarByClass((Class)FlowCountByDate.class);
        job.setMapperClass((Class)FlowMapper.class);
        job.setMapOutputKeyClass((Class)Text.class);
        job.setMapOutputValueClass((Class)LongWritable.class);
        job.setPartitionerClass((Class)KeyPartition.class);
        job.setCombinerClass((Class)FlowReduce.class);
        job.setReducerClass((Class)FlowReduce.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)LongWritable.class);
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
        help.printHelp("FlowCountByDate", options);
    }
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        ToolRunner.run(conf, (Tool)new FlowCountByDate(), args);
    }
    
    static {
        LOG = LogFactory.getLog((Class)FlowCountByDate.class);
    }
    
    public static class KeyPartition implements Partitioner<Text, LongWritable>
    {
        public int getPartition(final Text key, final LongWritable value, final int numPatitions) {
            return Math.abs(key.hashCode() * 127) % numPatitions;
        }
        
        public void configure(final JobConf arg0) {
        }
    }
    
    public static class FlowMapper extends MapReduceBase implements Mapper<Object, Text, Text, LongWritable>
    {
        public static final LongWritable longValue;
        
        public void map(final Object object, final Text value, final OutputCollector<Text, LongWritable> output, final Reporter reporter) throws IOException {
            final String str = value.toString();
            final String[] fieldValues = str.split("\\@\\#\\$");
            if (fieldValues.length != 8) {
                return;
            }
            final String area = fieldValues[2];
            final String flowstr = fieldValues[6];
            try {
                long flow = new Long(flowstr);
                flow /= 1024L;
                FlowMapper.longValue.set(flow);
            }
            catch (NumberFormatException e) {
                e.printStackTrace();
            }
            output.collect((Object)new Text(area), (Object)FlowMapper.longValue);
        }
        
        static {
            longValue = new LongWritable(1L);
        }
    }
    
    public static class FlowReduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>
    {
        public LongWritable total;
        
        public FlowReduce() {
            this.total = new LongWritable(0L);
        }
        
        public void reduce(final Text text, final Iterator<LongWritable> values, final OutputCollector<Text, LongWritable> output, final Reporter reporter) throws IOException {
            long sum = 0L;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            this.total.set(sum);
            output.collect((Object)text, (Object)this.total);
        }
    }
}
