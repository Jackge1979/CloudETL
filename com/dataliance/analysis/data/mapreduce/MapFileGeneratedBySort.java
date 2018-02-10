package com.dataliance.analysis.data.mapreduce;

import org.apache.hadoop.fs.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

public class MapFileGeneratedBySort extends Configured implements Tool
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
        MapFileGeneratedBySort.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        MapFileGeneratedBySort.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public JobConf createSubmittableJob(final CommandLine commands) throws IOException {
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final String outputPath = commands.getOptionValue("output") + System.currentTimeMillis();
        final JobConf job = new JobConf(this.getConf());
        job.setJobName("MapFileGeneratedBySort");
        job.setJarByClass((Class)MapFileGeneratedBySort.class);
        job.setMapperClass((Class)MapClass.class);
        job.setReducerClass((Class)Reduce.class);
        job.setPartitionerClass((Class)FirstPartitioner.class);
        job.setOutputValueGroupingComparator((Class)FirstGroupingComparator.class);
        job.setMapOutputKeyClass((Class)Phone2TimePair.class);
        job.setMapOutputValueClass((Class)Text.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)Text.class);
        job.setOutputFormat((Class)MapFileOutputFormat.class);
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
        help.printHelp("MapFileGeneratedBySort", options);
    }
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        ToolRunner.run(conf, (Tool)new MapFileGeneratedBySort(), args);
    }
    
    static {
        LOG = LogFactory.getLog((Class)MapFileGeneratedBySort.class);
    }
    
    public static class Phone2TimePair implements WritableComparable<Phone2TimePair>
    {
        private String first;
        private String second;
        
        public void set(final String first, final String second) {
            this.first = first;
            this.second = second;
        }
        
        public String getFirst() {
            return this.first;
        }
        
        public String getSecond() {
            return this.second;
        }
        
        public void setFirst(final String first) {
            this.first = first;
        }
        
        public void setSecond(final String second) {
            this.second = second;
        }
        
        public void readFields(final DataInput in) throws IOException {
            this.first = Text.readString(in);
            this.second = Text.readString(in);
        }
        
        public void write(final DataOutput out) throws IOException {
            Text.writeString(out, this.first);
            Text.writeString(out, this.second);
        }
        
        @Override
        public int hashCode() {
            return this.first.hashCode() + this.second.hashCode();
        }
        
        @Override
        public boolean equals(final Object o2) {
            if (o2 instanceof Phone2TimePair) {
                final Phone2TimePair r = (Phone2TimePair)o2;
                return this.first.equals(r.first) && this.second.equals(r.second);
            }
            return false;
        }
        
        public int compareTo(final Phone2TimePair o) {
            if (!this.first.equals(o.first)) {
                return this.first.compareTo(o.first);
            }
            if (!this.second.equals(o.second)) {
                return this.second.compareTo(o.second);
            }
            return 0;
        }
    }
    
    public static class FirstPartitioner implements Partitioner<Phone2TimePair, Text>
    {
        public int getPartition(final Phone2TimePair key, final Text value, final int numPartitions) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
        
        public void configure(final JobConf job) {
        }
    }
    
    public static class FirstGroupingComparator extends WritableComparator
    {
        protected FirstGroupingComparator() {
            super((Class)Phone2TimePair.class, true);
        }
        
        public int compare(final WritableComparable a, final WritableComparable b) {
            final Phone2TimePair pt1 = (Phone2TimePair)a;
            final Phone2TimePair pt2 = (Phone2TimePair)b;
            return pt1.getFirst().compareTo(pt2.getFirst());
        }
    }
    
    public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Phone2TimePair, Text>
    {
        private final Phone2TimePair key;
        
        public MapClass() {
            this.key = new Phone2TimePair();
        }
        
        public void map(final LongWritable inKey, final Text inValue, final OutputCollector<Phone2TimePair, Text> output, final Reporter reporter) throws IOException {
            if ("".equals(inValue.toString())) {
                return;
            }
            final String[] fields = inValue.toString().split(",");
            if (fields.length < 7) {
                return;
            }
            final String first = fields[0];
            final String second = fields[5];
            this.key.set(first, second);
            output.collect((Object)this.key, (Object)inValue);
        }
    }
    
    public static class Reduce extends MapReduceBase implements Reducer<Phone2TimePair, Text, Text, Text>
    {
        private final Text first;
        
        public Reduce() {
            this.first = new Text();
        }
        
        public void reduce(final Phone2TimePair key, final Iterator<Text> values, final OutputCollector<Text, Text> output, final Reporter reporter) throws IOException {
            while (values.hasNext()) {
                final Text value = values.next();
                this.first.set(key.getFirst());
                output.collect((Object)this.first, (Object)value);
            }
        }
    }
}
