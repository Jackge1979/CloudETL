package com.dataliance.analysis.data.region.mapreduce;

import org.apache.hadoop.fs.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.io.*;
import java.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;

public class RegionDistributionFilterByDate extends Configured implements Tool
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
        RegionDistributionFilterByDate.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        RegionDistributionFilterByDate.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public JobConf createSubmittableJob(final CommandLine commands) throws IOException {
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final String outputPath = commands.getOptionValue("output");
        final JobConf job = new JobConf(this.getConf());
        job.setJobName("RegionDistribution");
        job.setJarByClass((Class)RegionDistributionFilterByDate.class);
        job.setMapperClass((Class)MapClass.class);
        job.setCombinerClass((Class)CombinerClass.class);
        job.setReducerClass((Class)ReduceClass.class);
        job.setPartitionerClass((Class)FirstPartitioner.class);
        job.setMapOutputKeyClass((Class)RegionWritable.class);
        job.setMapOutputValueClass((Class)NullWritable.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)Text.class);
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
        ToolRunner.run(conf, (Tool)new RegionDistributionFilterByDate(), args);
    }
    
    static {
        LOG = LogFactory.getLog((Class)RegionDistributionFilterByDate.class);
    }
    
    public static class RegionWritable implements WritableComparable<RegionWritable>
    {
        private String region;
        private String telephone;
        
        public String getRegion() {
            return this.region;
        }
        
        public void setRegion(final String region) {
            this.region = region;
        }
        
        public String getTelephone() {
            return this.telephone;
        }
        
        public void setTelephone(final String telephone) {
            this.telephone = telephone;
        }
        
        public void readFields(final DataInput in) throws IOException {
            this.region = in.readUTF();
            this.telephone = in.readUTF();
        }
        
        public void write(final DataOutput out) throws IOException {
            out.writeUTF(this.region);
            out.writeUTF(this.telephone);
        }
        
        public int compareTo(final RegionWritable o) {
            final int cmp = o.region.compareTo(this.region);
            if (cmp != 0) {
                return cmp;
            }
            return o.telephone.compareTo(this.telephone);
        }
    }
    
    public static class FirstPartitioner implements Partitioner<RegionWritable, NullWritable>
    {
        public int getPartition(final RegionWritable key, final NullWritable value, final int numPatitions) {
            return Math.abs(key.getRegion().hashCode() * 127) % numPatitions;
        }
        
        public void configure(final JobConf arg0) {
        }
    }
    
    public static class MapClass extends MapReduceBase implements Mapper<Object, Text, RegionWritable, NullWritable>
    {
        public void map(final Object object, final Text value, final OutputCollector<RegionWritable, NullWritable> output, final Reporter reporter) throws IOException {
            final String str = value.toString();
            final String[] fieldValues = str.split("\\@\\#\\$");
            if (fieldValues.length != 8) {
                return;
            }
            final String telephone = fieldValues[1];
            final String area = fieldValues[2];
            final RegionWritable regionWritable = new RegionWritable();
            regionWritable.setRegion(area);
            regionWritable.setTelephone(telephone);
            output.collect((Object)regionWritable, (Object)NullWritable.get());
        }
    }
    
    public static class CombinerClass extends MapReduceBase implements Reducer<RegionWritable, NullWritable, RegionWritable, NullWritable>
    {
        public void reduce(final RegionWritable key, final Iterator<NullWritable> values, final OutputCollector<RegionWritable, NullWritable> output, final Reporter reporter) throws IOException {
            output.collect((Object)key, (Object)NullWritable.get());
        }
    }
    
    public static class ReduceClass extends MapReduceBase implements Reducer<RegionWritable, NullWritable, Text, Text>
    {
        public void reduce(final RegionWritable key, final Iterator<NullWritable> values, final OutputCollector<Text, Text> output, final Reporter reporter) throws IOException {
            final String region = key.getRegion();
            final String telephone = key.getTelephone();
            output.collect((Object)new Text(region), (Object)new Text(telephone));
        }
    }
}
