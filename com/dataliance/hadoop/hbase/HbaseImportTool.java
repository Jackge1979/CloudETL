package com.dataliance.hadoop.hbase;

import java.text.*;

import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.client.*;

import com.dataliance.hadoop.*;
import com.dataliance.hbase.util.*;
import com.dataliance.util.*;

import java.io.*;

public class HbaseImportTool extends AbstractTool
{
    @Override
    protected int doAction(final String[] args) throws Exception {
        final String className = this.clazz.getSimpleName();
        final String useAge = "Usage: " + className + "<tableIn> <tableOut>";
        if (args.length < 2) {
            System.err.println(useAge);
            return -1;
        }
        final String tableIn = args[0];
        final String tableOut = args[1];
        this.initJob();
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final long start = System.currentTimeMillis();
        HbaseImportTool.LOG.info(className + " : starting at " + sdf.format(start));
        this.init(tableIn, tableOut);
        this.initKey();
        final long end = System.currentTimeMillis();
        HbaseImportTool.LOG.info(className + " : finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
        return 0;
    }
    
    private void init(final String tableIn, final String tableOut) {
        this.getJobConf().set("hbase.mapreduce.inputtable", tableIn);
        this.setInputFormatClass((Class<? extends InputFormat>)TableInputFormat.class);
        this.getJobConf().set("hbase.mapred.outputtable", tableOut);
        this.setOutputFormatClass((Class<? extends OutputFormat>)TableOutputFormat.class);
    }
    
    private void initKey() {
        this.setOutputKeyClass(ImmutableBytesWritable.class);
    }
    
    protected void registScan(final Scan scan) throws IOException {
        this.getJobConf().set("hbase.mapreduce.scan", HbaseUtil.scanToString(scan));
    }
    
    protected void doAction() throws Exception {
    }
}
