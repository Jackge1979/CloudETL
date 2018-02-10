package com.dataliance.analysis.data.mapreduce;

import com.dataliance.analysis.data.classifier.*;
import com.dataliance.analysis.data.classifier.bean.*;
import com.dataliance.analysis.data.classifier.dao.*;
import com.dataliance.analysis.data.mapreduce.common.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;

import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class UrlBasedClassifier extends Configured implements Tool
{
    private static final Log LOG;
    public static final String OUTPUT_NAME = "url_based";
    private static CriterionUrlClassifier criterionUrlClassifier;
    private static List<String> filterKeywords;
    
    private static boolean filter(final String value) {
        boolean isFilter = false;
        for (final String keyword : UrlBasedClassifier.filterKeywords) {
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
        final BasicParser parser = new BasicParser();
        final CommandLine commands = parser.parse(options, args);
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
        UrlBasedClassifier.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        UrlBasedClassifier.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public Job createSubmittableJob(final CommandLine commands) throws IOException {
        final Configuration conf = this.getConf();
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final Path outputPath = new Path(commands.getOptionValue("output"), "url_based");
        final int reduceNum = Integer.parseInt(commands.getOptionValue("reduce", "6"));
        final FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        conf.set("value.field.indexs", commands.getOptionValue("fieldindex"));
        UrlBasedClassifier.LOG.info((Object)("input:" + commaSeparatedPaths));
        UrlBasedClassifier.LOG.info((Object)("output:" + outputPath));
        final Job job = new Job(this.getConf());
        job.setJobName(UrlBasedClassifier.class.getSimpleName());
        job.setJarByClass((Class)UrlBasedClassifier.class);
        job.setMapperClass((Class)ClassiferMapper.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)IntWritable.class);
        job.setInputFormatClass((Class)TextInputFormat.class);
        job.setOutputFormatClass((Class)ClassifiedOutputFormat.class);
        FileInputFormat.setInputPaths(job, commaSeparatedPaths);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setNumReduceTasks(0);
        return job;
    }
    
    public static final Options buildOptions() {
        final Options options = new Options();
        options.addOption("fieldindex", true, "[must] field position in line");
        options.addOption("input", true, "[must] input data of src");
        options.addOption("output", true, "[must] output data to dest ");
        return options;
    }
    
    public static final void printUsage(final Options options) {
        final HelpFormatter help = new HelpFormatter();
        help.printHelp(UrlBasedClassifier.class.getCanonicalName(), options);
    }
    
    public static void main(String[] args) throws Exception {
        args = new String[] { "-input", "./data/20120802-02001.TXT", "-output", "./classifier/" + System.currentTimeMillis(), "-fieldindex", "4" };
        final Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        final int exitCode = ToolRunner.run(conf, (Tool)new UrlBasedClassifier(), args);
        System.exit(exitCode);
    }
    
    static {
        LOG = LogFactory.getLog((Class)UrlBasedClassifier.class);
        UrlBasedClassifier.criterionUrlClassifier = null;
        UrlBasedClassifier.filterKeywords = new ArrayList<String>();
        try {
            final ICriterionUrlDao criterionUrlDao = new DBBasedCriterionUrlDao();
            final Map<Integer, List<CriterionUrl>> categoryId2urlPattern = criterionUrlDao.getAllCriterionUrlsForMap();
            UrlBasedClassifier.criterionUrlClassifier = new CriterionUrlClassifier(categoryId2urlPattern);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    static class ClassiferMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        String[] fieldIndexs;
        Text outKey;
        IntWritable outValue;
        
        ClassiferMapper() {
            this.outKey = new Text();
            this.outValue = new IntWritable();
        }
        
        protected void setup(final Mapper.Context context) throws IOException, InterruptedException {
            final Configuration conf = context.getConfiguration();
            this.fieldIndexs = conf.get("value.field.indexs", "1").split(",");
        }
        
        public void map(final LongWritable key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
            if ("".equals(value.toString().trim())) {
                return;
            }
            final String[] fieldValues = value.toString().split("@#\\$");
            final int fieldValueLength = fieldValues.length;
            final int keyIndex = Integer.parseInt(this.fieldIndexs[0].trim());
            if (keyIndex > fieldValueLength) {
                return;
            }
            final String keyField = fieldValues[keyIndex - 1];
            if (filter(keyField)) {
                return;
            }
            final int categoryLabel = UrlBasedClassifier.criterionUrlClassifier.doClassify(keyField);
            this.outKey.set(keyField);
            this.outValue.set(categoryLabel);
            context.write((Object)this.outKey, (Object)this.outValue);
        }
    }
    
    public static class UrlMergeReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        IntWritable firstValue;
        
        public UrlMergeReducer() {
            this.firstValue = new IntWritable();
        }
        
        public void reduce(final Text key, final Iterable<IntWritable> values, final Reducer.Context context) throws IOException, InterruptedException {
            final Iterator i$ = values.iterator();
            if (i$.hasNext()) {
                final IntWritable val = i$.next();
                this.firstValue = val;
            }
            context.write((Object)key, (Object)this.firstValue);
        }
    }
}
