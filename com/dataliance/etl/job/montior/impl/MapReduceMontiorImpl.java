package com.dataliance.etl.job.montior.impl;

import org.apache.hadoop.conf.*;
import java.io.*;

import com.dataliance.etl.job.montior.*;
import com.dataliance.etl.job.vo.*;
import com.dataliance.hadoop.manager.*;
import com.dataliance.util.*;

import org.apache.hadoop.mapred.*;

public class MapReduceMontiorImpl extends Configured implements MapReduceMontior
{
    private HadoopManager manager;
    private JobInfo jobInfo;
    
    public MapReduceMontiorImpl(final Configuration conf, final JobInfo jobInfo) throws IOException, InterruptedException {
        this(conf, new HadoopManager(conf), jobInfo);
    }
    
    public MapReduceMontiorImpl(final Configuration conf, final HadoopManager manager, final JobInfo jobInfo) {
        super(conf);
        this.manager = manager;
        this.jobInfo = jobInfo;
    }
    
    public MapRedeceJob getJobStatus() throws IOException {
        final String jobId = this.jobInfo.getJobID();
        if (!StringUtil.isEmpty(jobId)) {
            final JobStatus jobStatus = this.manager.getJobStatus(jobId);
            return this.parse(jobStatus);
        }
        if (this.jobInfo.getStatus() == JobInfo.STATUS.RUNNING) {
            return MapRedeceJob.getWiatJob(this.jobInfo.getJobName());
        }
        MapRedeceJob mrJob = null;
        switch (this.jobInfo.getStatus()) {
            case ERROR: {
                mrJob = MapRedeceJob.getError(MapRedeceJob.State.FAILED, this.jobInfo.getJobName());
                break;
            }
            case FINISH: {
                mrJob = MapRedeceJob.getError(MapRedeceJob.State.SUCCEEDED, this.jobInfo.getJobName());
                break;
            }
            case KILLED: {
                mrJob = MapRedeceJob.getError(MapRedeceJob.State.KILLED, this.jobInfo.getJobName());
                break;
            }
        }
        mrJob.setStartTime(this.jobInfo.getStartTime());
        mrJob.setEndTime(this.jobInfo.getEndTime());
        return mrJob;
    }
    
    private MapRedeceJob parse(final JobStatus jobStatus) {
        final MapRedeceJob job = new MapRedeceJob();
        job.setEndTime(this.jobInfo.getEndTime());
        job.setJobid(jobStatus.getJobID().toString());
        job.setJobName(this.jobInfo.getJobName());
        job.setMapComplete(jobStatus.mapProgress());
        job.setPriority(jobStatus.getJobPriority().name());
        job.setReduceComplete(jobStatus.reduceProgress());
        job.setStartTime(jobStatus.getStartTime());
        job.setState(MapRedeceJob.State.valueOf(JobStatus.getJobRunState(jobStatus.getRunState())));
        job.setUser(jobStatus.getUsername());
        return job;
    }
    
    public JobInfo.JOB_TYPE getType() {
        return JobInfo.JOB_TYPE.MAPREDUCE;
    }
    
    public MapReduceMontior toMapReduceMontior() throws IOException {
        return this;
    }
    
    public ProgramMonitor toProgramMonitor() throws IOException {
        throw MapReduceMontiorImpl.NOT_SUPPORT;
    }
    
    public boolean isFinish() {
        return this.jobInfo.getStatus() != JobInfo.STATUS.RUNNING;
    }
    
    public JobInfo.STATUS getStatus() {
        return this.jobInfo.getStatus();
    }
    
    public String getJobName() {
        return this.jobInfo.getJobName();
    }
}
