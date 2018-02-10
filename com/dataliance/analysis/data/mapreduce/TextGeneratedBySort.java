package com.dataliance.analysis.data.mapreduce;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.util.*;

public class TextGeneratedBySort extends Configured implements Tool
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
        final Job job = this.createSubmittableJob(commands);
        exitCode = (job.waitForCompletion(true) ? 0 : 1);
        endTime = System.currentTimeMillis();
        elapsedTime = (endTime - startTime) / 1000.0;
        TextGeneratedBySort.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        TextGeneratedBySort.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public Job createSubmittableJob(final CommandLine commands) throws IOException {
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final String outputPath = commands.getOptionValue("output") + System.currentTimeMillis();
        final Job job = new Job(this.getConf());
        job.setJobName("TextGeneratedBySort");
        job.setJarByClass((Class)TextGeneratedBySort.class);
        job.setMapperClass((Class)MapClass.class);
        job.setReducerClass((Class)Reduce.class);
        job.setPartitionerClass((Class)FirstPartitioner.class);
        job.setGroupingComparatorClass((Class)FirstGroupingComparator.class);
        job.setMapOutputKeyClass((Class)Phone2TimePair.class);
        job.setMapOutputValueClass((Class)Text.class);
        job.setOutputKeyClass((Class)NullWritable.class);
        job.setOutputValueClass((Class)Text.class);
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
        help.printHelp("TextGeneratedBySort", options);
    }
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        ToolRunner.run(conf, (Tool)new TextGeneratedBySort(), args);
    }
    
    static {
        LOG = LogFactory.getLog((Class)TextGeneratedBySort.class);
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
    
    public static class FirstPartitioner extends Partitioner<Phone2TimePair, Text>
    {
        public int getPartition(final Phone2TimePair key, final Text value, final int numPartitions) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
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
    
    public static class MapClass extends Mapper<LongWritable, Text, Phone2TimePair, Text>
    {
        private final Phone2TimePair key;
        
        public MapClass() {
            this.key = new Phone2TimePair();
        }
        
        public void map(final LongWritable inKey, final Text inValue, final Mapper.Context context) throws IOException, InterruptedException {
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
            context.write((Object)this.key, (Object)inValue);
        }
    }
    
    public static class Reduce extends Reducer<Phone2TimePair, Text, NullWritable, Text>
    {
        private final Text first;
        
        public Reduce() {
            this.first = new Text();
        }
        
        public void reduce(final Phone2TimePair key, final Iterable<Text> values, final Reducer.Context context) throws IOException, InterruptedException {
            for (final Text value : values) {
                this.first.set(key.getFirst());
                context.write((Object)NullWritable.get(), (Object)value);
            }
        }
    }
}
