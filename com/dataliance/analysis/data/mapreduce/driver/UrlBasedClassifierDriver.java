package com.dataliance.analysis.data.mapreduce.driver;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;

import com.dataliance.activemq.service.core.*;
import com.dataliance.analysis.data.mapreduce.*;
import com.dataliance.service.util.*;

import java.util.*;
import org.apache.commons.cli.*;
import org.slf4j.*;

public class UrlBasedClassifierDriver
{
    private static final Logger LOG;
    
    public static void runJob(final CommandLine commands) throws Exception {
        final Configuration conf = new Configuration();
        conf.addResource("bigdata-site.xml");
        final boolean isSendMessage = conf.getBoolean("auto.send.message.to.mq", false);
        final Path inputPath = new Path(commands.getOptionValue("input"));
        final Path outputPath = new Path(commands.getOptionValue("output"), System.currentTimeMillis() + "");
        final String fieldIndex = commands.getOptionValue("fieldindex");
        String[] args = { "-input", inputPath.toString(), "-output", outputPath.toString(), "-fieldindex", fieldIndex };
        UrlBasedClassifierDriver.LOG.info("UrlBasedClassifierDriver start ...");
        ToolRunner.run(conf, (Tool)new UrlBasedClassifier(), args);
        UrlBasedClassifierDriver.LOG.info("UrlClassifiedBasedImporter start ... ");
        Path classifiedInputPath = new Path(outputPath, "url_based");
        classifiedInputPath = new Path(classifiedInputPath, "classified");
        args = new String[] { "-input", classifiedInputPath.toString() };
        ToolRunner.run(conf, (Tool)new UrlClassifiedBasedImporter(), args);
        if (isSendMessage) {
            UrlBasedClassifierDriver.LOG.info("UrlBasedClassifierDriver send message to crawler ... ");
            Path unClassifiedPath = new Path(outputPath, "url_based");
            unClassifiedPath = new Path(unClassifiedPath, "un_classified");
            sendMessageToCrawler(unClassifiedPath);
        }
        UrlBasedClassifierDriver.LOG.info("process finished ... ");
    }
    
    private static void sendMessageToCrawler(final Path unClassifiedPath) {
        final Map<String, String> argName2value = new HashMap<String, String>();
        argName2value.put("jobnName", "sendToClawler");
        argName2value.put(JndiPropertyUtil.SERVICE_RESULT_PATH.UNCLIASSIFIED_URL_PATH.name(), unClassifiedPath.toString());
        MessageSender.get().sendMessage(JndiPropertyUtil.SERVICE_QUEUE.DataImportService.name(), argName2value);
        MessageSender.get().shutdown();
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
        help.printHelp(UrlBasedClassifierDriver.class.getSimpleName(), options);
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
        UrlBasedClassifierDriver.LOG.info("input:" + inputPath);
        UrlBasedClassifierDriver.LOG.info("output:" + outputPath);
        runJob(commands);
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)UrlBasedClassifierDriver.class);
    }
}
