package com.dataliance.analysis.data.mapreduce.driver;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import com.dataliance.analysis.data.mapreduce.*;
import org.apache.commons.cli.*;
import org.slf4j.*;

public class FieldStatisticDriver
{
    private static final Logger LOG;
    
    public static void runJob(final CommandLine commands) throws Exception {
        final Configuration conf = new Configuration();
        final Path inputPath = new Path(commands.getOptionValue("input"));
        final Path outputPath = new Path(commands.getOptionValue("output"));
        final String fieldIndex = commands.getOptionValue("fieldindex");
        String[] args = { "-input", inputPath.toString(), "-output", outputPath.toString(), "-fieldindex", fieldIndex };
        FieldStatisticDriver.LOG.info("FieldStatisticDriver start ...");
        ToolRunner.run(conf, (Tool)new FieldStatistic(), args);
        FieldStatisticDriver.LOG.info("DescendingOrder start ... ");
        final Path sortInputPath = new Path(outputPath, "statistic");
        System.out.println("sort path:" + sortInputPath.toString());
        args = new String[] { "-input", sortInputPath.toString(), "-output", outputPath.toString() };
        ToolRunner.run(conf, (Tool)new UserFrequencyDescendingOrder(), args);
        FieldStatisticDriver.LOG.info(" analysis finished ... ");
    }
    
    private static final Options buildOptions() {
        final Options options = new Options();
        options.addOption("fieldindex", true, "[must] field position in line");
        options.addOption("input", true, "[must] input data of src");
        options.addOption("output", true, "[option] output data to dest ");
        return options;
    }
    
    private static final void printUsage(final Options options) {
        final HelpFormatter help = new HelpFormatter();
        help.printHelp("FieldStatisticDriver", options);
    }
    
    public static void main(final String[] args) throws Exception {
        final Options options = buildOptions();
        final BasicParser parser = new BasicParser();
        CommandLine commands = null;
        try {
            commands = parser.parse(options, args);
        }
        catch (Exception e) {
            printUsage(options);
            return;
        }
        if (!commands.hasOption("input")) {
            printUsage(options);
            return;
        }
        if (!commands.hasOption("output")) {
            printUsage(options);
            return;
        }
        if (!commands.hasOption("fieldindex")) {
            printUsage(options);
            return;
        }
        final Path inputPath = new Path(commands.getOptionValue("input"));
        final Path outputPath = new Path(commands.getOptionValue("output"));
        FieldStatisticDriver.LOG.info("input:" + inputPath);
        FieldStatisticDriver.LOG.info("output:" + outputPath);
        runJob(commands);
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)FieldStatisticDriver.class);
    }
}
