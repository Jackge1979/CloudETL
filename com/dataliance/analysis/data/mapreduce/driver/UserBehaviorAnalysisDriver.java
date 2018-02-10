package com.dataliance.analysis.data.mapreduce.driver;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import com.dataliance.analysis.data.mapreduce.*;
import org.apache.commons.cli.*;
import org.slf4j.*;

public class UserBehaviorAnalysisDriver
{
    private static final Logger LOG;
    
    public static void runJob(final CommandLine commands) throws Exception {
        final Configuration conf = new Configuration();
        Path inputPath = new Path(commands.getOptionValue("input"));
        final Path outputPath = new Path(commands.getOptionValue("output"));
        String[] args = { "-input", inputPath.toString(), "-output", outputPath.toString() };
        UserBehaviorAnalysisDriver.LOG.info("UserVisitedUrlCollector start ...");
        ToolRunner.run(conf, (Tool)new UserVisitedUrlCollector(), args);
        UserBehaviorAnalysisDriver.LOG.info(" UserFrequencyStatistics pic url start ... ");
        final Path picInPath = new Path("collect", "pic_url");
        inputPath = new Path(outputPath, picInPath);
        inputPath = inputPath.getFileSystem(conf).makeQualified(inputPath);
        System.out.println("analysis path:" + inputPath.toString());
        final Path picOutPath = new Path(outputPath, "pic_url");
        args = new String[] { "-input", inputPath.toString(), "-output", picOutPath.toString() };
        ToolRunner.run(conf, (Tool)new UserFrequencyStatistics(), args);
        UserBehaviorAnalysisDriver.LOG.info(" UserFrequencyDescendingOrder pic url start ... ");
        Path sortInputPath = new Path(picOutPath, "frequency");
        System.out.println("sort path:" + sortInputPath.toString());
        args = new String[] { "-input", sortInputPath.toString(), "-output", picOutPath.toString() };
        ToolRunner.run(conf, (Tool)new UserFrequencyDescendingOrder(), args);
        UserBehaviorAnalysisDriver.LOG.info(" UserFrequencyStatistics text url start ... ");
        final Path textInPath = new Path("collect", "text_url");
        inputPath = new Path(outputPath, textInPath);
        inputPath = inputPath.getFileSystem(conf).makeQualified(inputPath);
        final Path textOutPath = new Path(outputPath, "text_url");
        args = new String[] { "-input", inputPath.toString(), "-output", textOutPath.toString() };
        ToolRunner.run(conf, (Tool)new UserFrequencyStatistics(), args);
        UserBehaviorAnalysisDriver.LOG.info(" UserFrequencyDescendingOrder text url start ... ");
        sortInputPath = new Path(textOutPath, "frequency");
        System.out.println("sort path:" + sortInputPath.toString());
        args = new String[] { "-input", sortInputPath.toString(), "-output", textOutPath.toString() };
        ToolRunner.run(conf, (Tool)new UserFrequencyDescendingOrder(), args);
        UserBehaviorAnalysisDriver.LOG.info(" analysis finished ... ");
    }
    
    private static final Options buildOptions() {
        final Options options = new Options();
        options.addOption("input", true, "[must] input data of src");
        options.addOption("output", true, "[option] output data to dest ");
        return options;
    }
    
    private static final void printUsage(final Options options) {
        final HelpFormatter help = new HelpFormatter();
        help.printHelp("UserBehaviorAnalysisDriver", options);
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
        final Path inputPath = new Path(commands.getOptionValue("input"));
        final Path outputPath = new Path(commands.getOptionValue("output"));
        UserBehaviorAnalysisDriver.LOG.info("input:" + inputPath);
        UserBehaviorAnalysisDriver.LOG.info("output:" + outputPath);
        runJob(commands);
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)UserBehaviorAnalysisDriver.class);
    }
}
