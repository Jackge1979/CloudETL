package com.dataliance.analysis.data.uv.mapreduce;

import org.apache.hadoop.fs.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.io.*;
import java.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

public class UVCountByDate extends Configured implements Tool
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
        UVCountByDate.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        UVCountByDate.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public JobConf createSubmittableJob(final CommandLine commands) throws IOException {
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final String outputPath = commands.getOptionValue("output");
        final JobConf job = new JobConf(this.getConf());
        job.setJobName("UVCountByDate");
        job.setJarByClass((Class)UVCountByDate.class);
        job.setMapperClass((Class)UVMapper.class);
        job.setMapOutputKeyClass((Class)TextPair.class);
        job.setMapOutputValueClass((Class)IntWritable.class);
        job.setPartitionerClass((Class)KeyPartition.class);
        job.setCombinerClass((Class)UVCombiner.class);
        job.setReducerClass((Class)UVReduce.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)IntWritable.class);
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
        help.printHelp("UVFliterByDate", options);
    }
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        ToolRunner.run(conf, (Tool)new UVCountByDate(), args);
    }
    
    static {
        LOG = LogFactory.getLog((Class)UVCountByDate.class);
    }
    
    public static class TextPair implements WritableComparable<TextPair>
    {
        private String region;
        private int hour;
        
        public String getRegion() {
            return this.region;
        }
        
        public void setRegion(final String region) {
            this.region = region;
        }
        
        public int getHour() {
            return this.hour;
        }
        
        public void setHour(final int hour) {
            this.hour = hour;
        }
        
        public void readFields(final DataInput in) throws IOException {
            this.region = in.readUTF();
            this.hour = in.readInt();
        }
        
        public void write(final DataOutput out) throws IOException {
            out.writeUTF(this.region);
            out.writeInt(this.hour);
        }
        
        public int compareTo(final TextPair o) {
            final int cmp = o.region.compareTo(this.region);
            if (cmp != 0) {
                return cmp;
            }
            return this.hour - o.hour;
        }
    }
    
    public static class KeyPartition implements Partitioner<TextPair, IntWritable>
    {
        public int getPartition(final TextPair key, final IntWritable value, final int numPatitions) {
            return Math.abs(key.getRegion().hashCode() * 127) % numPatitions;
        }
        
        public void configure(final JobConf arg0) {
        }
    }
    
    public static class UVMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, IntWritable>
    {
        public void map(final Object object, final Text value, final OutputCollector<TextPair, IntWritable> output, final Reporter reporter) throws IOException {
            final String str = value.toString();
            final String[] fieldValues = str.split("\\s+");
            if (fieldValues.length != 3) {
                return;
            }
            final String area = fieldValues[0];
            int hour = 0;
            try {
                hour = new Integer(fieldValues[1]);
            }
            catch (NumberFormatException e) {
                e.printStackTrace();
                return;
            }
            final TextPair tp = new TextPair();
            tp.setRegion(area);
            tp.setHour(hour);
            output.collect((Object)tp, (Object)new IntWritable(1));
        }
    }
    
    public static class UVCombiner extends MapReduceBase implements Reducer<TextPair, IntWritable, TextPair, IntWritable>
    {
        public IntWritable total;
        
        public UVCombiner() {
            this.total = new IntWritable(0);
        }
        
        public void reduce(final TextPair textPair, final Iterator<IntWritable> values, final OutputCollector<TextPair, IntWritable> output, final Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            this.total.set(sum);
            output.collect((Object)textPair, (Object)this.total);
        }
    }
    
    public static class UVReduce extends MapReduceBase implements Reducer<TextPair, IntWritable, Text, IntWritable>
    {
        public IntWritable total;
        
        public UVReduce() {
            this.total = new IntWritable(0);
        }
        
        public void reduce(final TextPair textPair, final Iterator<IntWritable> values, final OutputCollector<Text, IntWritable> output, final Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            this.total.set(sum);
            output.collect((Object)new Text(textPair.getRegion() + "\t" + textPair.getHour()), (Object)this.total);
        }
    }
}
