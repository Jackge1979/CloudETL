package com.dataliance.hbase.data.mapreduce;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.hbase.*;
import java.util.*;
import java.io.*;

public class RowCounter
{
    private static final Log LOG;
    private static final String NAME = "rowcounter";
    
    public static Job createSubmittableJob(final Configuration conf, final String[] args) throws IOException {
        final String tableName = args[0];
        final Job job = new Job(conf, "rowcounter_" + tableName);
        job.setJarByClass((Class)RowCounter.class);
        final StringBuilder sb = new StringBuilder();
        for (int i = 1; i < args.length; ++i) {
            if (i > 1) {
                sb.append(" ");
            }
            sb.append(args[i]);
        }
        final Scan scan = new Scan();
        scan.setFilter((Filter)new FirstKeyOnlyFilter());
        if (sb.length() > 0) {
            for (final String columnName : sb.toString().split(" ")) {
                final String[] fields = columnName.split(":");
                if (fields.length == 1) {
                    scan.addFamily(Bytes.toBytes(fields[0]));
                }
                else {
                    scan.addColumn(Bytes.toBytes(fields[0]), Bytes.toBytes(fields[1]));
                }
            }
        }
        job.setOutputFormatClass((Class)NullOutputFormat.class);
        TableMapReduceUtil.initTableMapperJob(tableName, scan, (Class)RowCounterMapper.class, (Class)ImmutableBytesWritable.class, (Class)Result.class, job);
        job.setNumReduceTasks(0);
        return job;
    }
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = HBaseConfiguration.create();
        final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 1) {
            System.err.println("ERROR: Wrong number of parameters: " + args.length);
            System.err.println("Usage: RowCounter <tablename> [<column1> <column2>...]");
            System.exit(-1);
        }
        final Job job = createSubmittableJob(conf, otherArgs);
        final int exitCode = job.waitForCompletion(true) ? 0 : 1;
        if (exitCode == 0) {
            final Counters counters = job.getCounters();
            final Counter counter = counters.findCounter((Enum)ROW_COUNTER.ROWS);
            RowCounter.LOG.info((Object)String.format("total count: %s", counter.getValue()));
        }
        System.exit(exitCode);
    }
    
    static {
        LOG = LogFactory.getLog((Class)RowCounter.class);
    }
    
    private enum ROW_COUNTER
    {
        ROWS;
    }
    
    static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result>
    {
        public void map(final ImmutableBytesWritable row, final Result values, final Mapper.Context context) throws IOException {
            for (final KeyValue value : values.list()) {
                if (value.getValue().length > 0) {
                    context.getCounter((Enum)ROW_COUNTER.ROWS).increment(1L);
                }
            }
        }
    }
}
