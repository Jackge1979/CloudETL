package com.dataliance.hadoop;

import org.apache.hadoop.util.*;
import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import java.text.*;
import java.util.*;
import java.util.logging.Logger;

import org.apache.hadoop.mapreduce.*;
import org.slf4j.*;

import com.dataliance.util.*;

public abstract class AbstractTool extends Configured implements Tool
{
    public static final Logger LOG;
    protected Job job;
    protected Class<?> clazz;
    
    public Job getJob() {
        return this.job;
    }
    
    public void setJob(final Job job) {
        this.job = job;
    }
    
    public AbstractTool() {
        this.clazz = this.getClass();
    }
    
    public AbstractTool(final Class<?> clazz) {
        this.clazz = clazz;
    }
    
    public void initJob() throws IOException {
        this.job = this.createJob();
    }
    
    public void initJob(final String jobName) throws IOException {
        this.job = this.createJob(jobName);
    }
    
    protected void initJob(final String jobName, final Class<? extends Mapper> mapClass, final Class<? extends Reducer> reduceClass) throws IOException {
        this.job = this.createJob(jobName);
        this.setMapperClass(mapClass);
        this.setReducerClass(reduceClass);
    }
    
    protected void initJob(final String jobName, final Path[] in, final Path out) throws IOException {
        this.job = this.createJob(jobName);
        this.addInputPath(in);
        this.setOutputPath(out);
    }
    
    protected void runJob(final boolean wait) throws Exception {
        this.job.waitForCompletion(wait);
    }
    
    private Job createJob() throws IOException {
        this.job = new Job(this.getConf());
        this.setJarByClass(this.clazz);
        return this.job;
    }
    
    protected Configuration getJobConf() {
        return this.job.getConfiguration();
    }
    
    private Job createJob(final String name) throws IOException {
        this.job = this.createJob();
        this.setJobName(name);
        return this.job;
    }
    
    protected void setInputFormatClass(final Class<? extends InputFormat> cls) {
        this.job.setInputFormatClass((Class)cls);
    }
    
    protected void setJobName(final String name) {
        this.job.setJobName(name);
    }
    
    protected void setJarByClass(final Class<?> cls) {
        this.job.setJarByClass((Class)cls);
    }
    
    protected void setNumReduceTasks(final int num) {
        this.job.setNumReduceTasks(num);
    }
    
    protected void setMapperClass(final Class<? extends Mapper> mapClass) {
        this.job.setMapperClass((Class)mapClass);
    }
    
    protected void setReducerClass(final Class<? extends Reducer> reduceClass) {
        this.job.setReducerClass((Class)reduceClass);
    }
    
    protected void setCombinerClass(final Class<? extends Reducer> reduceClass) {
        this.job.setCombinerClass((Class)reduceClass);
    }
    
    protected void setOutputFormatClass(final Class<? extends OutputFormat> cls) {
        this.job.setOutputFormatClass((Class)cls);
    }
    
    protected void setOutputPath(final Path path) {
        FileOutputFormat.setOutputPath(this.job, path);
    }
    
    protected void setTableName(final String tableName) {
        this.getJobConf().set("hbase.mapred.outputtable", tableName);
    }
    
    protected void setTableOutputFormat() {
        this.setOutputFormatClass((Class<? extends OutputFormat>)TableOutputFormat.class);
    }
    
    protected void setOutputValueClass(final Class<?> theClass) {
        this.job.setOutputValueClass((Class)theClass);
    }
    
    protected void setMapOutputKeyClass(final Class<?> theClass) {
        this.job.setMapOutputKeyClass((Class)theClass);
    }
    
    protected void setMapOutputValueClass(final Class<?> theClass) {
        this.job.setMapOutputValueClass((Class)theClass);
    }
    
    protected void setOutputKeyClass(final Class<?> theClass) {
        this.job.setOutputKeyClass((Class)theClass);
    }
    
    protected void addInputPath(final Path path) throws IOException {
        FileInputFormat.addInputPath(this.job, path);
    }
    
    protected void addInputPath(final Path[] path) throws IOException {
        for (final Path p : path) {
            FileInputFormat.addInputPath(this.job, p);
        }
    }
    
    public Class<?> getClazz() {
        return this.clazz;
    }
    
    public void setClazz(final Class<?> clazz) {
        this.clazz = clazz;
    }
    
    protected int doAction(final String[] args) throws Exception {
        final String className = this.clazz.getSimpleName();
        final String useAge = "Usage: " + className + " <inPath1> <inPath2> ... <destPath>";
        if (args.length < 2) {
            System.err.println(useAge);
            return -1;
        }
        final HashSet<Path> dirs = new HashSet<Path>();
        for (int i = 0; i < args.length - 1; ++i) {
            dirs.add(new Path(args[i]));
        }
        final Path destPath = new Path(args[args.length - 1]);
        if (this.job == null) {
            this.initJob(className + "-" + destPath);
        }
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (final Path p : dirs) {
            AbstractTool.LOG.info("Add in path : " + p);
        }
        AbstractTool.LOG.info("Set out path : " + destPath);
        final long start = System.currentTimeMillis();
        AbstractTool.LOG.info(className + " : starting at " + sdf.format(start));
        this.doAction(dirs.toArray(new Path[dirs.size()]), destPath);
        final long end = System.currentTimeMillis();
        AbstractTool.LOG.info(className + " : finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
        return 0;
    }
    
    protected void doAction(final Path[] in, final Path out) throws Exception {
    }
    
    public Counters getCounters() throws IOException {
        return this.job.getCounters();
    }
    
    public String getJobName() {
        return this.job.getJobName();
    }
    
    public String getJobID() {
        return this.job.getJobID().toString();
    }
    
    public int run(final String[] args) throws Exception {
        return this.doAction(args);
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)AbstractTool.class);
    }
}
