package com.dataliance.analysis.data.terminal.mapreduce;

import org.apache.hadoop.fs.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

import com.dataliance.hadoop.mapred.output.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.io.*;
import java.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

public class TerminalSortByDate extends Configured implements Tool
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
        TerminalSortByDate.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        TerminalSortByDate.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public JobConf createSubmittableJob(final CommandLine commands) throws IOException {
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final String outputPath = commands.getOptionValue("output");
        final JobConf job = new JobConf(this.getConf());
        job.setJobName("TerminalSort");
        job.setJarByClass((Class)TerminalCountByDate.class);
        job.setMapperClass((Class)MapClass.class);
        job.setMapOutputKeyClass((Class)TerminalWritable.class);
        job.setMapOutputValueClass((Class)NullWritable.class);
        job.setReducerClass((Class)ReduceClass.class);
        job.setOutputKeyClass((Class)IntWritable.class);
        job.setOutputValueClass((Class)Text.class);
        job.setNumReduceTasks(1);
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
        ToolRunner.run(conf, (Tool)new TerminalSortByDate(), args);
    }
    
    static {
        LOG = LogFactory.getLog((Class)TerminalSortByDate.class);
    }
    
    public static class TerminalWritable implements WritableComparable<TerminalWritable>
    {
        private int firstKey;
        private String secondValue;
        
        public int getFirstKey() {
            return this.firstKey;
        }
        
        public void setFirstKey(final int firstKey) {
            this.firstKey = firstKey;
        }
        
        public String getSecondValue() {
            return this.secondValue;
        }
        
        public void setSecondValue(final String secondValue) {
            this.secondValue = secondValue;
        }
        
        public void readFields(final DataInput in) throws IOException {
            this.firstKey = in.readInt();
            this.secondValue = in.readUTF();
        }
        
        public void write(final DataOutput out) throws IOException {
            out.writeInt(this.firstKey);
            out.writeUTF(this.secondValue);
        }
        
        public int compareTo(final TerminalWritable o) {
            final int thisKey = this.firstKey;
            final int thatKey = o.firstKey;
            return (thisKey < thatKey) ? 1 : ((thisKey == thatKey) ? this.secondValue.compareTo(o.secondValue) : -1);
        }
    }
    
    public static class MapClass extends MapReduceBase implements Mapper<Object, Text, TerminalWritable, NullWritable>
    {
        private TerminalWritable terminalWritable;
        
        public MapClass() {
            this.terminalWritable = new TerminalWritable();
        }
        
        public void map(final Object object, final Text value, final OutputCollector<TerminalWritable, NullWritable> output, final Reporter reporter) throws IOException {
            final String str = value.toString();
            final String[] arr = str.split("@@@");
            if (arr.length != 2) {
                System.out.println("=====" + str);
                return;
            }
            final int firstKey = new Integer(arr[0]);
            this.terminalWritable.setFirstKey(firstKey);
            this.terminalWritable.setSecondValue(arr[1]);
            output.collect((Object)this.terminalWritable, (Object)NullWritable.get());
        }
    }
    
    public static class ReduceClass extends MapReduceBase implements Reducer<TerminalWritable, NullWritable, IntWritable, Text>
    {
        public void reduce(final TerminalWritable terminalWritable, final Iterator<NullWritable> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException {
            final int firstKey = terminalWritable.getFirstKey();
            final String secondValue = terminalWritable.getSecondValue();
            output.collect((Object)new IntWritable(firstKey), (Object)new Text(firstKey + "@@@" + secondValue));
        }
    }
}
