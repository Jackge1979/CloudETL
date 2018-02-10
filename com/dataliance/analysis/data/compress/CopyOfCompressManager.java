package com.dataliance.analysis.data.compress;

import org.apache.hadoop.conf.*;
import java.io.*;
import com.cms.framework.model.bigdata.*;
import com.dataliance.hadoop.*;
import com.dataliance.main.*;
import com.dataliance.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;

import java.util.*;

public class CopyOfCompressManager extends Configured
{
    private static Map<Long, Job> jobs;
    private static Map<Long, StringBuffer> logs;
    private String jarName;
    private FileSystem fs;
    
    public CopyOfCompressManager(final Configuration conf) throws IOException {
        super(conf);
        this.jarName = "/opt/brainbook/bigdata-core/bigdata-core-1.0.1.jar";
        this.jarName = conf.get("com.DA.compress.jarname", "/opt/brainbook/bigdata-core/bigdata-core-1.0.1.jar");
        this.fs = FileSystem.get(conf);
    }
    
    public String submitJob(final Compression compress) throws Throwable {
        final CopyOfCompressConvert cCon = new CopyOfCompressConvert(new Path(compress.getInput()), new Path(compress.getOutput()));
        cCon.setFormat(compress.getFormat());
        cCon.setRetainOriginalFile(compress.isRetainOriginalFile());
        cCon.setConf(this.getConf());
        cCon.initJob();
        final Job job = RunJar.runJar(this.getConf(), this.jarName, cCon, new String[0]);
        if (job != null) {
            CopyOfCompressManager.jobs.put(compress.getId(), job);
            final StringBuffer sb = new StringBuffer();
            sb.append("jobName:" + job.getJobName()).append("\n");
            final JobID jobID = job.getJobID();
            if (jobID != null) {
                sb.append("jobId  :" + job.getJobID()).append("\n");
                sb.append("status : map " + job.mapProgress() + "% reduce" + job.reduceProgress()).append("\n");
            }
            return sb.toString();
        }
        return null;
    }
    
    public boolean outExists(final String out) throws IOException {
        return this.fs.exists(new Path(out));
    }
    
    public String getLog(final long id) throws IOException {
        final Job job = CopyOfCompressManager.jobs.get(id);
        if (job != null) {
            final StringBuffer sb = CopyOfCompressManager.logs.get(id);
            this.appendLog(sb, job);
            return sb.toString();
        }
        return "Job where id = " + id + " is not in monitor!";
    }
    
    private void appendLog(final StringBuffer sb, final Job job) throws IOException {
        if (!job.isComplete()) {
            final JobID jobID = job.getJobID();
            if (jobID != null) {
                sb.append("status : map " + job.mapProgress() + "% reduce" + job.reduceProgress()).append("\n");
            }
        }
    }
    
    public boolean isFinish(final long id) throws IOException {
        final Job job = CopyOfCompressManager.jobs.get(id);
        return job == null || job.isComplete();
    }
    
    public static void main(final String[] args) throws Throwable {
        final String usage = "<in> <out> <compress>";
        if (args.length < 3) {
            System.err.println(usage);
            return;
        }
        final Configuration conf = DAConfigUtil.create();
        final CopyOfCompressManager manager = new CopyOfCompressManager(conf);
        final Compression compression = new Compression();
        compression.setId(0L);
        compression.setInput(args[0]);
        compression.setOutput(args[1]);
        compression.setFormat(args[2]);
        System.out.println(manager.submitJob(compression));
    }
    
    static {
        CopyOfCompressManager.jobs = new HashMap<Long, Job>();
        CopyOfCompressManager.logs = new HashMap<Long, StringBuffer>();
    }
}
