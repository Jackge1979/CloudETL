package com.dataliance.analysis.data.mapreduce;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import java.util.*;
import org.apache.hadoop.mapreduce.*;

public class FieldStatistic extends Configured implements Tool
{
    private static final Log LOG;
    public static final String OUTPUT_NAME = "statistic";
    private static List<String> filterKeywords;
    
    private static boolean filter(final String value) {
        boolean isFilter = false;
        for (final String keyword : FieldStatistic.filterKeywords) {
            if (value.endsWith(keyword) || value.endsWith(keyword.toUpperCase())) {
                isFilter = true;
                break;
            }
        }
        return isFilter;
    }
    
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
        if (!commands.hasOption("fieldindex")) {
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
        FieldStatistic.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        FieldStatistic.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public Job createSubmittableJob(final CommandLine commands) throws IOException {
        final Configuration conf = this.getConf();
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final Path outputPath = new Path(commands.getOptionValue("output"), "statistic");
        final FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        conf.set("value.field.indexs", commands.getOptionValue("fieldindex"));
        final Job job = new Job(conf);
        job.setJobName("statistic");
        job.setJarByClass((Class)FieldStatistic.class);
        FileInputFormat.setInputPaths(job, commaSeparatedPaths);
        FileOutputFormat.setOutputPath(job, outputPath);
        FieldStatistic.LOG.info((Object)("commaSeparatedPaths:" + commaSeparatedPaths));
        job.setInputFormatClass((Class)TextInputFormat.class);
        job.setOutputFormatClass((Class)SequenceFileOutputFormat.class);
        job.setMapperClass((Class)SiteMapper.class);
        job.setCombinerClass((Class)SiteSumCombiner.class);
        job.setReducerClass((Class)SiteSumReducer.class);
        job.setMapOutputKeyClass((Class)Text.class);
        job.setMapOutputValueClass((Class)LongWritable.class);
        job.setOutputKeyClass((Class)LongWritable.class);
        job.setOutputValueClass((Class)Text.class);
        return job;
    }
    
    private static final Options buildOptions() {
        final Options options = new Options();
        options.addOption("fieldindex", true, "[must] field position in line");
        options.addOption("input", true, "[must] input directory of data");
        options.addOption("output", true, "[must] target directory of data");
        return options;
    }
    
    private static final void printUsage(final Options options) {
        final HelpFormatter help = new HelpFormatter();
        help.printHelp("fieldstatistic", options);
    }
    
    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        args = new String[] { "-fieldindex", "25", "-input", "./data/2012021503494000000123311.txt", "-output", "./statistic" };
        final int exitCode = ToolRunner.run(conf, (Tool)new FieldStatistic(), args);
        System.exit(exitCode);
    }
    
    static {
        LOG = LogFactory.getLog((Class)FieldStatistic.class);
        (FieldStatistic.filterKeywords = new ArrayList<String>()).add("png");
        FieldStatistic.filterKeywords.add("jpeg");
        FieldStatistic.filterKeywords.add("gif");
        FieldStatistic.filterKeywords.add("jpg");
        FieldStatistic.filterKeywords.add("bmp");
        FieldStatistic.filterKeywords.add("tif");
    }
    
    public static class SiteMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        private static final LongWritable one;
        String splitTag;
        String[] fieldIndexs;
        Text outKey;
        Text outValue;
        
        public SiteMapper() {
            this.splitTag = ",";
            this.outKey = new Text();
            this.outValue = new Text();
        }
        
        public void setup(final Mapper.Context context) {
            final Configuration conf = context.getConfiguration();
            this.fieldIndexs = conf.get("value.field.indexs", "1").split(",");
        }
        
        public void map(final LongWritable key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
            if ("".equals(value.toString().trim())) {
                return;
            }
            final String[] fieldValues = value.toString().split(this.splitTag);
            final int fieldValueLength = fieldValues.length;
            final int keyIndex = Integer.parseInt(this.fieldIndexs[0].trim());
            if (keyIndex > fieldValueLength) {
                return;
            }
            final String keyField = fieldValues[keyIndex - 1];
            if (filter(keyField)) {
                return;
            }
            this.outKey.set(keyField);
            context.write((Object)this.outKey, (Object)SiteMapper.one);
        }
        
        static {
            one = new LongWritable(1L);
        }
    }
    
    public static class SiteSumCombiner extends Reducer<Text, LongWritable, Text, LongWritable>
    {
        private LongWritable result;
        
        public SiteSumCombiner() {
            this.result = new LongWritable();
        }
        
        public void reduce(final Text key, final Iterable<LongWritable> values, final Reducer.Context context) throws IOException, InterruptedException {
            long sum = 0L;
            for (final LongWritable val : values) {
                sum += val.get();
            }
            this.result.set(sum);
            context.write((Object)key, (Object)this.result);
        }
    }
    
    public static class SiteSumReducer extends Reducer<Text, LongWritable, LongWritable, Text>
    {
        private LongWritable result;
        
        public SiteSumReducer() {
            this.result = new LongWritable();
        }
        
        public void reduce(final Text key, final Iterable<LongWritable> values, final Reducer.Context context) throws IOException, InterruptedException {
            long sum = 0L;
            for (final LongWritable val : values) {
                sum += val.get();
            }
            this.result.set(sum);
            context.write((Object)this.result, (Object)key);
        }
    }
}
