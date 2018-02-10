package com.dataliance.analysis.data.mapreduce;

import org.apache.hadoop.io.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import java.util.*;
import java.io.*;

public class LookupRecordsByPhoneNumber extends Configured implements Tool
{
    private static final Log LOG;
    
    public int run(final String[] args) throws Exception {
        final int exitCode = -1;
        final Options options = buildOptions();
        CommandLine commands = null;
        try {
            final BasicParser parser = new BasicParser();
            commands = parser.parse(options, args);
        }
        catch (ParseException e2) {
            printUsage(options);
            return exitCode;
        }
        if (!commands.hasOption("phonenumber")) {
            printUsage(options);
            return -1;
        }
        if (!commands.hasOption("input")) {
            printUsage(options);
            return -1;
        }
        if (!commands.hasOption("output")) {
            printUsage(options);
            return -1;
        }
        final Path input = new Path(commands.getOptionValue("input"));
        final Path output = new Path(commands.getOptionValue("output"));
        final String phoneNumber = commands.getOptionValue("phonenumber");
        LookupRecordsByPhoneNumber.LOG.info((Object)("input path:" + input.toString()));
        LookupRecordsByPhoneNumber.LOG.info((Object)("output path:" + output.toString()));
        final MapFileGeneratedBySort.Phone2TimePair phoneTimePair = new MapFileGeneratedBySort.Phone2TimePair();
        phoneTimePair.setFirst(phoneNumber);
        final Text key = new Text(phoneNumber);
        final Text value = new Text();
        FSDataOutputStream out = null;
        MapFile.Reader reader = null;
        try {
            final FileSystem fs = input.getFileSystem(this.getConf());
            final MapFile.Reader[] readers = MapFileOutputFormatAddFilter.getReaders(fs, input, this.getConf());
            final Partitioner<MapFileGeneratedBySort.Phone2TimePair, Text> partitioner = (Partitioner<MapFileGeneratedBySort.Phone2TimePair, Text>)new MapFileGeneratedBySort.FirstPartitioner();
            reader = readers[partitioner.getPartition((Object)phoneTimePair, (Object)value, readers.length)];
            out = fs.create(output, true);
            long count = 0L;
            final Text nextKey = new Text();
            do {
                if (key.equals((Object)nextKey)) {
                    out.writeBytes(value.toString() + "\n");
                }
                ++count;
            } while (reader.next((WritableComparable)nextKey, (Writable)value));
            System.out.println("count:" + count);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            IOUtils.closeStream((Closeable)out);
            IOUtils.closeStream((Closeable)reader);
        }
        return 0;
    }
    
    private static final Options buildOptions() {
        final Options options = new Options();
        options.addOption("input", true, "[must] input data path");
        options.addOption("output", true, "[must] output data path");
        options.addOption("phonenumber", true, "[must] input phone number");
        return options;
    }
    
    private static final void printUsage(final Options options) {
        final HelpFormatter help = new HelpFormatter();
        help.printHelp("LookupRecordByPhoneNumber", options);
    }
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final long startTime = System.currentTimeMillis();
        long endTime = 0L;
        double elapsedTime = 0.0;
        ToolRunner.run(conf, (Tool)new LookupRecordsByPhoneNumber(), args);
        endTime = System.currentTimeMillis();
        elapsedTime = (endTime - startTime) / 1000.0;
        LookupRecordsByPhoneNumber.LOG.info((Object)String.format("elapsedTime %s seconds", elapsedTime));
        LookupRecordsByPhoneNumber.LOG.info((Object)"finished...!");
    }
    
    static {
        LOG = LogFactory.getLog((Class)LookupRecordsByPhoneNumber.class);
    }
    
    private static class MapFileOutputFormatAddFilter extends MapFileOutputFormat
    {
        public static MapFile.Reader[] getReaders(final FileSystem ignored, final Path dir, final Configuration conf) throws IOException {
            final FileSystem fs = dir.getFileSystem(conf);
            final Path[] allNames = FileUtil.stat2Paths(fs.listStatus(dir));
            final List<Path> valids = new ArrayList<Path>();
            for (final Path path : allNames) {
                if (!path.getName().startsWith("_") && !path.getName().startsWith(".")) {
                    valids.add(path);
                }
            }
            final Path[] names = valids.toArray(new Path[0]);
            Arrays.sort(names);
            final MapFile.Reader[] parts = new MapFile.Reader[names.length];
            for (int i = 0; i < names.length; ++i) {
                parts[i] = new MapFile.Reader(fs, names[i].toString(), conf);
            }
            return parts;
        }
    }
}
