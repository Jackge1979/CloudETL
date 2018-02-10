package com.dataliance.analysis.data.mapreduce;

import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import com.dataliance.analysis.data.mapreduce.common.*;
import org.apache.hadoop.conf.*;
import java.io.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class UserVisitedUrlCollector extends Configured implements Tool
{
    private static final Log LOG;
    public static final String OUTPUT_NAME = "collect";
    private static List<String> keywords;
    
    private static boolean filter(final String value) {
        for (final String keyword : UserVisitedUrlCollector.keywords) {
            if (value.contains(keyword)) {
                return false;
            }
        }
        return true;
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
        final long startTime = System.currentTimeMillis();
        long endTime = 0L;
        double elapsedTime = 0.0;
        final Job job = this.createSubmittableJob(commands);
        exitCode = (job.waitForCompletion(true) ? 0 : 1);
        endTime = System.currentTimeMillis();
        elapsedTime = (endTime - startTime) / 1000.0;
        UserVisitedUrlCollector.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        UserVisitedUrlCollector.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public Job createSubmittableJob(final CommandLine commands) throws IOException {
        final Configuration conf = this.getConf();
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final Path outputPath = new Path(commands.getOptionValue("output"), "collect");
        final FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        conf.set("filter.enable", commands.getOptionValue("filter.enable", "false"));
        final Job job = new Job(conf);
        job.setJobName("UserVisitedUrlCollector");
        job.setJarByClass((Class)UserVisitedUrlCollector.class);
        LogPathFilter.setConf(conf);
        FileInputFormat.setInputPathFilter(job, (Class)LogPathFilter.class);
        FileInputFormat.setInputPaths(job, commaSeparatedPaths);
        FileOutputFormat.setOutputPath(job, outputPath);
        UserVisitedUrlCollector.LOG.info((Object)("commaSeparatedPaths:" + commaSeparatedPaths));
        job.setInputFormatClass((Class)TextInputFormat.class);
        job.setMapperClass((Class)SiteMapper.class);
        job.setMapOutputKeyClass((Class)Text.class);
        job.setMapOutputValueClass((Class)Text.class);
        job.setOutputFormatClass((Class)UrlTypeOutputFormat.class);
        job.setReducerClass((Class)SiteReducer.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)Text.class);
        return job;
    }
    
    private static final Options buildOptions() {
        final Options options = new Options();
        options.addOption("input", true, "[must] input directory of data");
        options.addOption("output", true, "[must] target directory of data");
        options.addOption("isfilter", true, "[option] value filter is enable");
        return options;
    }
    
    private static final void printUsage(final Options options) {
        final HelpFormatter help = new HelpFormatter();
        help.printHelp("UserVisitedUrlCollector", options);
    }
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final int exitCode = ToolRunner.run(conf, (Tool)new UserVisitedUrlCollector(), args);
        System.exit(exitCode);
    }
    
    static {
        LOG = LogFactory.getLog((Class)UserVisitedUrlCollector.class);
        (UserVisitedUrlCollector.keywords = new ArrayList<String>()).add("sports");
    }
    
    private static class LogPathFilter implements PathFilter
    {
        static FileSystem fs;
        static Configuration conf;
        boolean accepted;
        
        private LogPathFilter() {
            this.accepted = true;
        }
        
        static void setConf(final Configuration _conf) {
            LogPathFilter.conf = _conf;
        }
        
        public boolean accept(final Path path) {
            FileStatus fStatus = null;
            try {
                LogPathFilter.fs = FileSystem.get(LogPathFilter.conf);
                fStatus = LogPathFilter.fs.getFileStatus(path);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            if (!fStatus.isDir()) {
                this.accepted = path.getName().endsWith(".txt");
            }
            return this.accepted;
        }
    }
    
    public static class SiteMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        String splitTag;
        String[] fieldIndexs;
        StringBuffer buffer;
        Text outKey;
        Text outValue;
        boolean isFilter;
        
        public SiteMapper() {
            this.splitTag = ",";
            this.outKey = new Text();
            this.outValue = new Text();
            this.isFilter = false;
            this.buffer = new StringBuffer();
        }
        
        public void setup(final Mapper.Context context) {
            final Configuration conf = context.getConfiguration();
            this.fieldIndexs = conf.get("value.field.indexs", "25, 1").split(",");
            this.isFilter = conf.getBoolean("filter.enable", false);
        }
        
        public void map(final LongWritable key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
            if ("".equals(value.toString().trim())) {
                return;
            }
            final String[] fieldValues = value.toString().split(this.splitTag);
            final int fieldValueLength = fieldValues.length;
            final int fieldLenth = this.fieldIndexs.length;
            if (fieldLenth != 2) {
                return;
            }
            final int keyIndex = Integer.parseInt(this.fieldIndexs[0].trim());
            final int valueIndex = Integer.parseInt(this.fieldIndexs[1].trim());
            if (keyIndex > fieldValueLength || valueIndex > fieldValueLength) {
                return;
            }
            final String visitedUrl = fieldValues[keyIndex - 1];
            final String phoneNumber = fieldValues[valueIndex - 1];
            if (this.isFilter && filter(visitedUrl)) {
                return;
            }
            this.outKey.set(visitedUrl);
            this.outValue.set(phoneNumber);
            context.write((Object)this.outKey, (Object)this.outValue);
        }
    }
    
    public static class SiteReducer extends Reducer<Text, Text, Text, Text>
    {
        StringBuilder buffer;
        Text outValue;
        
        public SiteReducer() {
            this.buffer = new StringBuilder();
            this.outValue = new Text();
        }
        
        protected void setup(final Reducer.Context context) throws IOException, InterruptedException {
        }
        
        protected void reduce(final Text key, final Iterable<Text> values, final Reducer.Context context) throws IOException, InterruptedException {
            for (final Text value : values) {
                context.write((Object)key, (Object)value);
            }
        }
    }
}
