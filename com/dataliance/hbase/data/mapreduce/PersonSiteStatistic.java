package com.dataliance.hbase.data.mapreduce;

import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.conf.*;
import java.io.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class PersonSiteStatistic extends Configured implements Tool
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
        PersonSiteStatistic.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        PersonSiteStatistic.LOG.info((Object)"finished...!");
        return exitCode;
    }
    
    public Job createSubmittableJob(final CommandLine commands) throws IOException {
        final String commaSeparatedPaths = commands.getOptionValue("input");
        final String outputPath = commands.getOptionValue("output") + System.currentTimeMillis();
        final Configuration conf = this.getConf();
        final String indexs = conf.get("value.field.indexs", "0");
        System.out.println("indexs:" + indexs);
        final Job job = new Job(conf);
        job.setJobName("statistic");
        job.setJarByClass((Class)PersonSiteStatistic.class);
        LogPathFilter.setConf(conf);
        FileInputFormat.setInputPathFilter(job, (Class)LogPathFilter.class);
        FileInputFormat.setInputPaths(job, commaSeparatedPaths);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        PersonSiteStatistic.LOG.info((Object)("commaSeparatedPaths:" + commaSeparatedPaths));
        job.setInputFormatClass((Class)TextInputFormat.class);
        job.setMapperClass((Class)SiteMapper.class);
        job.setMapOutputKeyClass((Class)Text.class);
        job.setMapOutputValueClass((Class)Text.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)Text.class);
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
        help.printHelp("sitestatistic", options);
    }
    
    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        args = new String[] { "-input", "./data/2012021510464700000123311.txt", "-output", "./data/extract" };
        final int exitCode = ToolRunner.run(conf, (Tool)new PersonSiteStatistic(), args);
        System.exit(exitCode);
    }
    
    static {
        LOG = LogFactory.getLog((Class)PersonSiteStatistic.class);
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
        String[] keyFieldIndexs;
        StringBuffer buffer;
        
        public SiteMapper() {
            this.splitTag = ",";
            this.buffer = new StringBuffer();
        }
        
        public void setup(final Mapper.Context context) {
            final Configuration conf = context.getConfiguration();
            this.keyFieldIndexs = conf.get("value.field.indexs", "0,5").split(",");
        }
        
        public void map(final LongWritable key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
            if ("".equals(value.toString().trim())) {
                return;
            }
            this.buffer.setLength(0);
            final String[] fieldValues = value.toString().split(this.splitTag);
            final int fieldValueLength = fieldValues.length;
            final int keyFieldLenth = this.keyFieldIndexs.length;
            int index = -1;
            for (int i = 0; i < keyFieldLenth; ++i) {
                index = Integer.parseInt(this.keyFieldIndexs[i].trim());
                if (index >= fieldValueLength) {
                    return;
                }
                this.buffer.append(fieldValues[index]);
                if (i < keyFieldLenth - 1) {
                    this.buffer.append("-");
                }
            }
            context.write((Object)new Text(this.buffer.toString()), (Object)value);
        }
    }
}
