package com.dataliance.analysis.data.mapreduce;

import org.apache.hadoop.conf.*;
import org.apache.lucene.analysis.tokenattributes.*;
import java.io.*;
import org.apache.lucene.analysis.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import org.wltea.analyzer.lucene.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import com.dataliance.nutch.segment.*;

public class DocumentWordSegment extends Configured implements Tool
{
    private static final Log LOG;
    private static final String NAME = "DocumentWordSegment";
    private static final String WORD_SPLIT = " ";
    private static Analyzer analyzer;
    
    public static String getTokens(final String content, final String splitTag) {
        final StringBuilder buffer = new StringBuilder();
        final TokenStream tokenStream = DocumentWordSegment.analyzer.tokenStream((String)null, (Reader)new StringReader(content));
        final CharTermAttribute termAtt = (CharTermAttribute)tokenStream.addAttribute((Class)CharTermAttribute.class);
        try {
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                final char[] termBuffer = termAtt.buffer();
                final int termLen = termAtt.length();
                if (termLen < 2) {
                    continue;
                }
                buffer.append(termBuffer, 0, termLen);
                buffer.append(splitTag);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return buffer.toString();
    }
    
    public int run(final String[] args) throws Exception {
        final int exitCode = -1;
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
        if (!commands.hasOption("inputpath")) {
            printUsage(options);
            return -1;
        }
        if (!commands.hasOption("outputpath")) {
            printUsage(options);
            return -1;
        }
        final String inputPath = commands.getOptionValue("inputpath");
        final String outputPath = commands.getOptionValue("outputpath");
        DocumentWordSegment.LOG.info((Object)("inputpath:" + inputPath));
        DocumentWordSegment.LOG.info((Object)("outputpath:" + outputPath));
        final Job job = new Job(this.getConf());
        job.setJobName("DocumentWordSegment");
        job.setJarByClass((Class)DocumentWordSegment.class);
        job.setMapperClass((Class)WordSegmentMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)Text.class);
        job.setInputFormatClass((Class)TextInputFormat.class);
        job.setOutputFormatClass((Class)TextOutputFormat.class);
        final Path inputDir = new Path(inputPath);
        final Path outputDir = new Path(outputPath);
        FileInputFormat.setInputPaths(job, new Path[] { inputDir });
        FileOutputFormat.setOutputPath(job, outputDir);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    private static final Options buildOptions() {
        final Options options = new Options();
        options.addOption("inputpath", true, "[must] input data of src");
        options.addOption("outputpath", true, "[must] output data to dest ");
        return options;
    }
    
    private static final void printUsage(final Options options) {
        final HelpFormatter help = new HelpFormatter();
        help.printHelp("WordSegment", options);
    }
    
    public static void main(final String[] args) throws Exception {
        final String content = "\u7535\u5b50\u901a\u4fe1\u7c7b\u677f\u5757\u8dcc\u5e45\u8f83\u5927\uff0c\u800c\u524d\u671f\u7684\u8fde\u7eed\u4e0b\u8dcc\u7684\u5730\u4ea7\u5374\u8d85\u8dcc\u53cd\u5f39\u4e86\u3002\u8fd9\u5c31\u5145\u5206\u53cd\u6620\u4e86\u80a1\u5e02\u91cc\u6da8\u4e45\u5fc5\u8dcc\uff0c\u8dcc\u4e45\u5fc5\u6da8\u7684\u9053\u7406.";
        final String result = getTokens(content, " ");
        System.out.println(result);
        final int exitCode = ToolRunner.run((Tool)new DocumentWordSegment(), args);
        System.exit(exitCode);
    }
    
    static {
        LOG = LogFactory.getLog((Class)DocumentWordSegment.class);
        DocumentWordSegment.analyzer = (Analyzer)new IKAnalyzer();
    }
    
    static class WordSegmentMapper extends Mapper<LongWritable, WebDocument, Text, Text>
    {
        protected void setup(final Mapper.Context context) throws IOException, InterruptedException {
        }
        
        protected void map(final LongWritable key, final WebDocument value, final Mapper.Context context) throws IOException, InterruptedException {
            final String words = DocumentWordSegment.getTokens(value.getContent(), " ");
            context.write((Object)new Text(value.getUrl()), (Object)new Text(words));
        }
    }
}
