package com.dataliance.analysis.data.mapreduce;

import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.commons.cli.*;
import org.apache.commons.logging.*;

public class UserFrequencyDescendingOrder extends Configured implements Tool
{
    private static final Log LOG;
    public static final String OUTPUT_NAME = "sorted";
    
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
        UserFrequencyDescendingOrder.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        UserFrequencyDescendingOrder.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public Job createSubmittableJob(final CommandLine commands) throws IOException {
        final Configuration conf = this.getConf();
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final Path outputPath = new Path(commands.getOptionValue("output"), "sorted");
        final FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        final Job job = new Job(conf);
        job.setJobName("UserFrequencyDescendingOrder");
        job.setJarByClass((Class)UserFrequencyDescendingOrder.class);
        FileInputFormat.setInputPaths(job, commaSeparatedPaths);
        FileOutputFormat.setOutputPath(job, outputPath);
        UserFrequencyDescendingOrder.LOG.info((Object)("commaSeparatedPaths:" + commaSeparatedPaths));
        job.setInputFormatClass((Class)SequenceFileInputFormat.class);
        job.setOutputFormatClass((Class)TextOutputFormat.class);
        job.setSortComparatorClass((Class)LongWritable.DecreasingComparator.class);
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
        help.printHelp("UserFrequencyDescendingOrder", options);
    }
    
    static {
        LOG = LogFactory.getLog((Class)UserFrequencyDescendingOrder.class);
    }
}
