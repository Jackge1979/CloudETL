package com.dataliance.etl.job.manager;

import org.apache.hadoop.conf.*;
import java.io.*;

import com.dataliance.etl.inject.rpc.*;
import com.dataliance.etl.inject.rpc.impl.*;
import com.dataliance.etl.job.montior.*;
import com.dataliance.etl.job.montior.impl.*;
import com.dataliance.etl.job.option.*;
import com.dataliance.etl.job.vo.*;
import com.dataliance.hadoop.manager.*;

import java.net.*;
import org.apache.hadoop.ipc.*;

import com.dataliance.service.util.*;

import org.slf4j.*;

public class JobManager extends Configured
{
    private static final Logger LOG;
    private static HadoopManager manager;
    private JobOption jobOption;
    
    public static Montior getTestMontior() {
        return new TestJobMonitorImpl();
    }
    
    public static Montior getTestMontior(final JobInfo.JOB_TYPE type) {
        if (type == JobInfo.JOB_TYPE.PROGRAM) {
            return new TestJobMonitorImpl();
        }
        return new TestMapReduceMontior();
    }
    
    public JobManager(final Configuration conf) throws IOException, InterruptedException {
        super(conf);
        this.jobOption = new JobOption(conf);
        if (JobManager.manager == null) {
            JobManager.manager = new HadoopManager(conf);
        }
    }
    
    public void kill(final String id) throws IOException {
        final JobInfo jobInfo = this.jobOption.get(id);
        if (jobInfo.getType() == JobInfo.JOB_TYPE.MAPREDUCE) {
            JobManager.manager.killJob(jobInfo.getJobID());
            jobInfo.setEndTime(System.currentTimeMillis());
            jobInfo.setStatus(JobInfo.STATUS.KILLED);
            this.jobOption.insert(jobInfo);
        }
        else if (jobInfo.getStatus() == JobInfo.STATUS.RUNNING) {
            try {
                final Manager proManager = (Manager)RPC.getProxy((Class)Manager.class, 1L, new InetSocketAddress(jobInfo.getHost(), jobInfo.getPort()), this.getConf());
                proManager.close();
            }
            catch (Exception e) {
                JobManager.LOG.error(e.getMessage(), (Throwable)e);
            }
        }
    }
    
    public Montior getMontior(final String id) throws IOException {
        JobInfo jobInfo = this.jobOption.get(id);
        if (jobInfo.getType().equals(JobInfo.JOB_TYPE.MAPREDUCE)) {
            return new MapReduceMontiorImpl(this.getConf(), JobManager.manager, jobInfo);
        }
        if (jobInfo.getStatus().equals(JobInfo.STATUS.RUNNING)) {
            try {
                return new ProRunMonitorImpl(jobInfo.getJobName(), this.getConf(), jobInfo.getHost(), jobInfo.getPort());
            }
            catch (Exception e) {
                JobManager.LOG.error(e.getMessage(), (Throwable)e);
                jobInfo = this.jobOption.get(id);
                return new ProFinishMonitor(jobInfo, jobInfo.getJobData());
            }
        }
        if (jobInfo.getStatus() != null) {
            return new ProFinishMonitor(jobInfo, jobInfo.getJobData());
        }
        throw new IOException("The job where id = " + id + " is not start! Please wait amount!");
    }
    
    public static void main(final String[] args) throws IOException, InterruptedException {
        final String usAge = "Usage : -get <id>";
        if (args.length == 0) {
            System.err.println(usAge);
            return;
        }
        final Configuration conf = ConfigUtils.getConfig();
        if (!args[0].equals("-get")) {
            System.err.println(usAge);
            return;
        }
        String id = "";
        if (args.length == 2) {
            id = args[1];
        }
        final JobManager manager = new JobManager(conf);
        final Montior montior = manager.getMontior(id);
        if (JobInfo.JOB_TYPE.MAPREDUCE == montior.getType()) {
            final MapReduceMontior mapReduceMontior = montior.toMapReduceMontior();
            final MapRedeceJob job = mapReduceMontior.getJobStatus();
            System.out.println("getJobid = " + job.getJobid());
            System.out.println("getJobName = " + job.getJobName());
            System.out.println("getMapComplete = " + job.getMapComplete());
            System.out.println("getReduceComplete = " + job.getReduceComplete());
            System.out.println("getState = " + job.getState());
        }
        else {
            final ProgramMonitor proMontior = montior.toProgramMonitor();
            System.out.println("proMontior.getJobName() = " + proMontior.getJobName());
        }
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)JobManager.class);
    }
}
