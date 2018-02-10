package com.dataliance.hadoop.hbase;

import java.util.*;
import org.apache.hadoop.fs.*;
import java.text.*;

import com.dataliance.hadoop.*;
import com.dataliance.util.*;

import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.*;
import java.io.*;

public class HbaseTool extends AbstractTool
{
    @Override
    protected int doAction(final String[] args) throws Exception {
        final String className = this.clazz.getSimpleName();
        final String useAge = "Usage: " + className + " <inPath1> <inPath2> ... <tableName>";
        if (args.length < 2) {
            System.err.println(useAge);
            return -1;
        }
        final HashSet<Path> dirs = new HashSet<Path>();
        for (int i = 0; i < args.length - 1; ++i) {
            dirs.add(new Path(args[i]));
        }
        final String tableName = args[args.length - 1];
        this.initJob();
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final long start = System.currentTimeMillis();
        HbaseTool.LOG.info(className + " : starting at " + sdf.format(start));
        this.init(tableName);
        this.initOutput();
        this.doAction(dirs.toArray(new Path[dirs.size()]));
        final long end = System.currentTimeMillis();
        HbaseTool.LOG.info(className + " : finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
        return 0;
    }
    
    private void initOutput() {
        this.setOutputKeyClass(ImmutableBytesWritable.class);
        this.setOutputValueClass(Put.class);
    }
    
    private void init(final String tableName) throws IOException {
        TableMapReduceUtil.initTableReducerJob(tableName, (Class)null, this.job, (Class)HRegionPartitioner.class);
    }
    
    protected void doAction(final Path[] in) throws Exception {
    }
}
