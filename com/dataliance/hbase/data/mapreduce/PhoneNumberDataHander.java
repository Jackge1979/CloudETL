package com.dataliance.hbase.data.mapreduce;

import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class PhoneNumberDataHander extends Configured implements Tool
{
    private static final Log LOG;
    private static final String NAME = "randomPhoneNumber";
    private static final int MSISDN_INDEX = 1;
    private static final int TOTAL_LEN = 32;
    
    private static String generateRandomTelephoneNumber() {
        final StringBuilder buffer = new StringBuilder();
        final Random ran = new Random();
        buffer.append(1);
        final int temp = ran.nextInt(3) * 25 / 10 + 3;
        buffer.append(temp);
        buffer.append((temp == 8) ? (ran.nextInt(3) + 6) : ran.nextInt(10));
        buffer.append(ran.nextInt(90000000) + 10000000);
        return buffer.toString();
    }
    
    private static String joinArrayToString(final String[] arrays) {
        final StringBuilder buffer = new StringBuilder();
        final int length = arrays.length;
        int count = 1;
        for (final String value : arrays) {
            buffer.append(String.format("'%s'", value));
            if (count < length) {
                buffer.append(",");
            }
            ++count;
        }
        return buffer.toString();
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
        final Configuration conf = this.getConf();
        if (!commands.hasOption("inputdir")) {
            printUsage(options);
            return -1;
        }
        if (!commands.hasOption("outputdir")) {
            printUsage(options);
            return -1;
        }
        final String inputData = commands.getOptionValue("inputdir");
        final String outputData = commands.getOptionValue("outputdir");
        PhoneNumberDataHander.LOG.info((Object)("inputdir:" + inputData));
        PhoneNumberDataHander.LOG.info((Object)("outputdir:" + outputData));
        final long startTime = System.currentTimeMillis();
        long endTime = 0L;
        double elapsedTime = 0.0;
        final Job job = this.createSubmittableJob(conf, commands);
        exitCode = (job.waitForCompletion(true) ? 0 : 1);
        endTime = System.currentTimeMillis();
        elapsedTime = (endTime - startTime) / 1000.0;
        PhoneNumberDataHander.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        PhoneNumberDataHander.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public Job createSubmittableJob(final Configuration conf, final CommandLine commands) throws IOException {
        final Path inputDir = new Path(commands.getOptionValue("inputdir"));
        final Path outputDir = new Path(commands.getOptionValue("outputdir"));
        final Job job = new Job(conf, "randomPhoneNumber");
        job.setJarByClass((Class)PhoneNumberDataHander.class);
        FileInputFormat.setInputPaths(job, new Path[] { inputDir });
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setInputFormatClass((Class)TextInputFormat.class);
        job.setOutputFormatClass((Class)TextOutputFormat.class);
        job.setMapperClass((Class)PhoneNumberRandomMapper.class);
        job.setOutputKeyClass((Class)NullWritable.class);
        job.setOutputValueClass((Class)Text.class);
        job.setNumReduceTasks(0);
        return job;
    }
    
    private static final Options buildOptions() {
        final Options options = new Options();
        options.addOption("inputdir", true, "[must] target directory of data");
        options.addOption("outputdir", true, "[must] dest directory of data");
        return options;
    }
    
    private static final void printUsage(final Options options) {
        final HelpFormatter help = new HelpFormatter();
        help.printHelp("PhoneNumberDataHander", options);
    }
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = HBaseConfiguration.create();
        final int exitCode = ToolRunner.run(conf, (Tool)new PhoneNumberDataHander(), args);
        System.exit(exitCode);
    }
    
    static {
        LOG = LogFactory.getLog((Class)PhoneNumberDataHander.class);
    }
    
    static class PhoneNumberRandomMapper extends Mapper<LongWritable, Text, NullWritable, Text>
    {
        public void map(final LongWritable key, final Text value, final Mapper.Context context) throws IOException {
            try {
                if ("".equals(value.toString().trim())) {
                    return;
                }
                final String[] fieldValues = value.toString().split("','");
                final int fieldValueLength = fieldValues.length;
                final int fieldLength = 32;
                if (fieldValueLength < fieldLength) {
                    PhoneNumberDataHander.LOG.info((Object)("skip invalid record :" + value.toString()));
                    return;
                }
                for (int i = 0; i < fieldValueLength; ++i) {
                    fieldValues[i] = fieldValues[i].replace("'", "");
                }
                fieldValues[1] = generateRandomTelephoneNumber();
                context.write((Object)NullWritable.get(), (Object)new Text(joinArrayToString(fieldValues)));
            }
            catch (InterruptedException e) {
                PhoneNumberDataHander.LOG.info((Object)("failure line:" + value.toString()));
                e.printStackTrace();
            }
        }
    }
}
