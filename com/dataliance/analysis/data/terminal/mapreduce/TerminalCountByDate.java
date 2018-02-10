package com.dataliance.analysis.data.terminal.mapreduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;

import java.io.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

import com.dataliance.hadoop.mapred.output.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

public class TerminalCountByDate extends Configured implements Tool
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
        TerminalCountByDate.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        TerminalCountByDate.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public JobConf createSubmittableJob(final CommandLine commands) throws IOException {
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final String outputPath = commands.getOptionValue("output");
        final JobConf job = new JobConf(this.getConf());
        job.setJobName("TerminalCount");
        job.setJarByClass((Class)TerminalCountByDate.class);
        job.setMapperClass((Class)CountMapClass.class);
        job.setMapOutputKeyClass((Class)Text.class);
        job.setMapOutputValueClass((Class)IntWritable.class);
        job.setCombinerClass((Class)CombineClass.class);
        job.setReducerClass((Class)CountReduceClass.class);
        job.setOutputKeyClass((Class)IntWritable.class);
        job.setOutputValueClass((Class)Text.class);
        job.setSpeculativeExecution(false);
        FileInputFormat.setInputPaths(job, commaSeparatedPaths);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormat((Class)LineOutputFormat.class);
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
        ToolRunner.run(conf, (Tool)new TerminalCountByDate(), args);
    }
    
    static {
        LOG = LogFactory.getLog((Class)TerminalCountByDate.class);
    }
    
    public static class CountMapClass extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable>
    {
        public Text textKey;
        public static final IntWritable intValue;
        
        public CountMapClass() {
            this.textKey = new Text("key");
        }
        
        public void map(final Object object, final Text value, final OutputCollector<Text, IntWritable> output, final Reporter reporter) throws IOException {
            final String str = value.toString();
            final String[] fieldValues = str.split("\\@\\#\\$");
            if (fieldValues.length != 8) {
                return;
            }
            String terminal = fieldValues[5];
            if (terminal == null || terminal.equals("")) {
                terminal = "unknown terminal";
            }
            this.textKey.set(terminal);
            output.collect((Object)this.textKey, (Object)CountMapClass.intValue);
        }
        
        static {
            intValue = new IntWritable(1);
        }
    }
    
    public static class CombineClass extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
    {
        public IntWritable total;
        
        public CombineClass() {
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
    
    public static class CountReduceClass extends MapReduceBase implements Reducer<Text, IntWritable, IntWritable, Text>
    {
        public IntWritable total;
        
        public CountReduceClass() {
            this.total = new IntWritable(0);
        }
        
        public void reduce(final Text key, final Iterator<IntWritable> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            this.total.set(sum);
            output.collect((Object)this.total, (Object)new Text(this.total + "@@@" + key));
        }
    }
}
