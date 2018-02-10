package com.dataliance.analysis.data.mapreduce;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import com.dataliance.bigdata.hbase.mapper.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;

public class UrlClassifiedBasedImporter extends Configured implements Tool
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
        final Configuration conf = this.getConf();
        if (!commands.hasOption("input")) {
            printUsage(options);
            return -1;
        }
        final String inputDir = commands.getOptionValue("input");
        UrlClassifiedBasedImporter.LOG.info((Object)("input:" + inputDir));
        final long startTime = System.currentTimeMillis();
        long endTime = 0L;
        double elapsedTime = 0.0;
        final Job job = this.createSubmittableJob(conf, commands);
        exitCode = (job.waitForCompletion(true) ? 0 : 1);
        endTime = System.currentTimeMillis();
        elapsedTime = (endTime - startTime) / 1000.0;
        UrlClassifiedBasedImporter.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        UrlClassifiedBasedImporter.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public Job createSubmittableJob(final Configuration conf, final CommandLine commands) throws IOException {
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final Job job = new Job(conf);
        job.setJobName(UrlClassifiedBasedImporter.class.getSimpleName());
        job.setJarByClass((Class)UrlClassifiedBasedImporter.class);
        FileInputFormat.setInputPaths(job, commaSeparatedPaths);
        UrlClassifiedBasedImporter.LOG.info((Object)("commaSeparatedPaths:" + commaSeparatedPaths));
        job.setInputFormatClass((Class)TextInputFormat.class);
        job.setMapperClass((Class)BasedUrlImportMapper.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)Text.class);
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        job.setOutputFormatClass((Class)NullOutputFormat.class);
        job.setNumReduceTasks(0);
        return job;
    }
    
    private static final Options buildOptions() {
        final Options options = new Options();
        options.addOption("input", true, "[must] target directory of data");
        return options;
    }
    
    private static final void printUsage(final Options options) {
        final HelpFormatter help = new HelpFormatter();
        help.printHelp(UrlClassifiedBasedImporter.class.getSimpleName(), options);
    }
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = HBaseConfiguration.create();
        conf.addResource("bigdata-site.xml");
        final int exitCode = ToolRunner.run(conf, (Tool)new UrlClassifiedBasedImporter(), args);
        System.exit(exitCode);
    }
    
    static {
        LOG = LogFactory.getLog((Class)UrlClassifiedBasedImporter.class);
    }
}
