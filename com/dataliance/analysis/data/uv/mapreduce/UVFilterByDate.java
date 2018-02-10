package com.dataliance.analysis.data.uv.mapreduce;

import org.apache.hadoop.fs.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import java.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import java.util.*;

public class UVFilterByDate extends Configured implements Tool
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
        UVFilterByDate.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        UVFilterByDate.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public JobConf createSubmittableJob(final CommandLine commands) throws IOException {
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final String outputPath = commands.getOptionValue("output");
        final JobConf job = new JobConf(this.getConf());
        job.setJobName("UVFliterByDate");
        job.setJarByClass((Class)UVFilterByDate.class);
        job.setMapperClass((Class)UVMapper.class);
        job.setMapOutputKeyClass((Class)TextPair.class);
        job.setMapOutputValueClass((Class)NullWritable.class);
        job.setPartitionerClass((Class)KeyPartition.class);
        job.setCombinerClass((Class)UVCombiner.class);
        job.setReducerClass((Class)UVReduce.class);
        job.setOutputKeyClass((Class)Text.class);
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
        help.printHelp("UVFliterByDate", options);
    }
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        ToolRunner.run(conf, (Tool)new UVFilterByDate(), args);
    }
    
    static {
        LOG = LogFactory.getLog((Class)UVFilterByDate.class);
    }
    
    public static class TextPair implements WritableComparable<TextPair>
    {
        private String region;
        private int hour;
        private String telephone;
        
        public String getTelephone() {
            return this.telephone;
        }
        
        public void setTelephone(final String telephone) {
            this.telephone = telephone;
        }
        
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
            this.telephone = in.readUTF();
        }
        
        public void write(final DataOutput out) throws IOException {
            out.writeUTF(this.region);
            out.writeInt(this.hour);
            out.writeUTF(this.telephone);
        }
        
        public int compareTo(final TextPair o) {
            final int cmp = o.region.compareTo(this.region);
            if (cmp != 0) {
                return cmp;
            }
            final int t = this.hour - o.hour;
            if (t != 0) {
                return t;
            }
            return o.telephone.compareTo(this.telephone);
        }
    }
    
    public static class KeyPartition implements Partitioner<TextPair, NullWritable>
    {
        public int getPartition(final TextPair key, final NullWritable value, final int numPatitions) {
            return Math.abs(key.getRegion().hashCode() * 127) % numPatitions;
        }
        
        public void configure(final JobConf arg0) {
        }
    }
    
    public static class UVMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, NullWritable>
    {
        public void map(final Object object, final Text value, final OutputCollector<TextPair, NullWritable> output, final Reporter reporter) throws IOException {
            final String str = value.toString();
            final String[] fieldValues = str.split("\\@\\#\\$");
            if (fieldValues.length != 8) {
                return;
            }
            final String date = fieldValues[0].substring(8, 10);
            final int hour = new Integer(date);
            final String area = fieldValues[2];
            final String telephone = fieldValues[1];
            final TextPair tp = new TextPair();
            tp.setRegion(area);
            tp.setHour(hour);
            tp.setTelephone(telephone);
            output.collect((Object)tp, (Object)NullWritable.get());
        }
    }
    
    public static class UVCombiner extends MapReduceBase implements Reducer<TextPair, IntWritable, TextPair, NullWritable>
    {
        public void reduce(final TextPair textPair, final Iterator<IntWritable> values, final OutputCollector<TextPair, NullWritable> output, final Reporter reporter) throws IOException {
            output.collect((Object)textPair, (Object)NullWritable.get());
        }
    }
    
    public static class UVReduce extends MapReduceBase implements Reducer<TextPair, NullWritable, Text, Text>
    {
        public IntWritable total;
        
        public UVReduce() {
            this.total = new IntWritable(0);
        }
        
        public void reduce(final TextPair textPair, final Iterator<NullWritable> values, final OutputCollector<Text, Text> output, final Reporter reporter) throws IOException {
            output.collect((Object)new Text(textPair.getRegion() + "\t" + textPair.getHour()), (Object)new Text(textPair.getTelephone()));
        }
    }
}
