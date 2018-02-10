package com.dataliance.etl.job.montior;

import org.apache.hadoop.conf.*;

import com.dataliance.etl.job.option.*;
import com.dataliance.etl.job.vo.*;
import com.dataliance.util.*;

import org.apache.hadoop.mapreduce.*;
import java.net.*;
import java.io.*;

public class JobMontior extends Configured
{
    public static final String CONF_JOB_SERVICE_HOST = "com.DA.job.service";
    private String jobService;
    private JobOption jobOption;
    private String programId;
    private JobInfo jobInfo;
    private Job job;
    private Thread t;
    private String taskName;
    private int index;
    private int total;
    
    public JobMontior(final String programId, final Job job, final String taskName, final int index, final int total) throws UnknownHostException {
        super(job.getConfiguration());
        this.programId = programId;
        this.jobInfo = new JobInfo();
        this.jobOption = new JobOption(this.getConf());
        this.job = job;
        this.jobService = this.getConf().get("com.DA.job.service", "http://" + InetAddress.getLocalHost().getHostName() + ":8080/updateTaskStatus");
        this.taskName = taskName;
        this.index = index;
        this.total = total;
    }
    
    public void start() throws IOException {
        this.jobInfo.setType(JobInfo.JOB_TYPE.MAPREDUCE);
        this.jobInfo.setJobName(this.job.getJobName());
        this.jobInfo.setProgramID(this.programId);
        this.jobInfo.setStatus(JobInfo.STATUS.RUNNING);
        this.jobInfo.setStartTime(System.currentTimeMillis());
        this.jobInfo.setIndex(this.index);
        this.jobInfo.setTaskName(this.taskName);
        final JobID jobID = this.job.getJobID();
        if (jobID != null) {
            this.jobInfo.setJobID(jobID.toString());
        }
        else {
            (this.t = new JobIDGeter()).start();
        }
        this.doPush(JobInfo.STATUS.RUNNING);
        this.jobOption.insert(this.jobInfo);
    }
    
    public void stop() throws IOException {
        this.jobInfo.setEndTime(System.currentTimeMillis());
        try {
            if (this.job.isSuccessful()) {
                this.jobInfo.setStatus(JobInfo.STATUS.FINISH);
                this.doPush(JobInfo.STATUS.FINISH);
            }
            else {
                this.jobInfo.setStatus(JobInfo.STATUS.ERROR);
                this.doPush(JobInfo.STATUS.ERROR);
            }
        }
        catch (Exception e) {
            this.jobInfo.setStatus(JobInfo.STATUS.FINISH);
            this.doPush(JobInfo.STATUS.FINISH);
        }
        this.jobOption.insert(this.jobInfo);
    }
    
    private void doPush(final JobInfo.STATUS status) throws IOException {
        URL url = null;
        if (status == JobInfo.STATUS.FINISH) {
            if (this.index == this.total) {
                url = new URL(this.jobService + "?id=" + this.programId + "&taskName=" + this.taskName + "&status=" + JobInfo.STATUS.FINISH.ordinal());
            }
            else {
                url = new URL(this.jobService + "?id=" + this.programId + "&taskName=" + this.taskName);
            }
        }
        else {
            url = new URL(this.jobService + "?id=" + this.programId + "&taskName=" + this.taskName + "&status=" + status.ordinal());
        }
        final BufferedReader br = StreamUtil.getBufferedReader(url.openStream());
        final String line = br.readLine();
        if (line != null && line.equals("0")) {
            System.out.println("SUCCESS");
        }
        else {
            System.out.println("ERROR");
        }
        br.close();
    }
    
    public void stop(final JobInfo.STATUS status) throws IOException {
        this.jobInfo.setEndTime(System.currentTimeMillis());
        this.jobInfo.setStatus(status);
        this.jobOption.insert(this.jobInfo);
        if (this.t.isAlive()) {
            this.t.interrupt();
        }
    }
    
    class JobIDGeter extends Thread
    {
        @Override
        public void run() {
            JobID jobID = JobMontior.this.job.getJobID();
            while (jobID == null) {
                try {
                    Thread.sleep(1000L);
                    jobID = JobMontior.this.job.getJobID();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                JobMontior.this.jobInfo.setJobID(jobID.toString());
                JobMontior.this.jobOption.insert(JobMontior.this.jobInfo);
            }
            catch (IOException e2) {
                e2.printStackTrace();
            }
        }
    }
}
