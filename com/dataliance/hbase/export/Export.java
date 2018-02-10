package com.dataliance.hbase.export;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import com.dataliance.hbase.mapreduce.*;

import org.apache.hadoop.hbase.client.*;
import java.io.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.commons.logging.*;

public class Export
{
    private static final Log LOG;
    static final String NAME = "export";
    
    public static Job createSubmittableJob(final Configuration conf, final String[] args) throws IOException {
        final String tableName = args[0];
        final Path outputDir = new Path(args[1]);
        final Job job = new Job(conf, "export_" + tableName);
        job.setJobName("export_" + tableName);
        job.setJarByClass((Class)DAExporter.class);
        final Scan s = getConfiguredScanForJob(conf, args);
        TableMapReduceUtil.initTableMapperJob(tableName, s, (Class)DAExporter.class, (Class)null, (Class)null, job);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass((Class)SequenceFileOutputFormat.class);
        job.setOutputKeyClass((Class)ImmutableBytesWritable.class);
        job.setOutputValueClass((Class)Result.class);
        FileOutputFormat.setOutputPath(job, outputDir);
        return job;
    }
    
    private static Scan getConfiguredScanForJob(final Configuration conf, final String[] args) throws IOException {
        final Scan s = new Scan();
        final int versions = (args.length > 2) ? Integer.parseInt(args[2]) : 1;
        s.setMaxVersions(versions);
        final long startTime = (args.length > 3) ? Long.parseLong(args[3]) : 0L;
        final long endTime = (args.length > 4) ? Long.parseLong(args[4]) : Long.MAX_VALUE;
        s.setTimeRange(startTime, endTime);
        s.setCacheBlocks(false);
        if (conf.get("hbase.mapreduce.scan.column.family") != null) {
            s.addFamily(Bytes.toBytes(conf.get("hbase.mapreduce.scan.column.family")));
        }
        final Filter exportFilter = getExportFilter(args);
        if (exportFilter != null) {
            Export.LOG.info((Object)"Setting Scan Filter for Export.");
            s.setFilter(exportFilter);
        }
        Export.LOG.info((Object)("verisons=" + versions + ", starttime=" + startTime + ", endtime=" + endTime));
        return s;
    }
    
    private static Filter getExportFilter(final String[] args) {
        Filter exportFilter = null;
        final String filterCriteria = (args.length > 5) ? args[5] : null;
        if (filterCriteria == null) {
            return null;
        }
        if (filterCriteria.startsWith("^")) {
            final String regexPattern = filterCriteria.substring(1, filterCriteria.length());
            exportFilter = (Filter)new RowFilter(CompareFilter.CompareOp.EQUAL, (WritableByteArrayComparable)new RegexStringComparator(regexPattern));
        }
        else {
            exportFilter = (Filter)new PrefixFilter(Bytes.toBytes(filterCriteria));
        }
        return exportFilter;
    }
    
    private static void usage(final String errorMsg) {
        if (errorMsg != null && errorMsg.length() > 0) {
            System.err.println("ERROR: " + errorMsg);
        }
        System.err.println("Usage: Export [-D <property=value>]* <tablename> <outputdir> [<versions> [<starttime> [<endtime>]] [^[regex pattern] or [Prefix] to filter]]\n");
        System.err.println("  Note: -D properties will be applied to the conf used. ");
        System.err.println("  For example: ");
        System.err.println("   -D mapred.output.compress=true");
        System.err.println("   -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec");
        System.err.println("   -D mapred.output.compression.type=BLOCK");
        System.err.println("  Additionally, the following SCAN properties can be specified");
        System.err.println("  to control/limit what is exported..");
        System.err.println("   -D hbase.mapreduce.scan.column.family=<familyName>");
    }
    
    static {
        LOG = LogFactory.getLog((Class)Export.class);
    }
}
