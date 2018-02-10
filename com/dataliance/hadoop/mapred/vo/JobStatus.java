package com.dataliance.hadoop.mapred.vo;

import org.apache.hadoop.mapred.*;

public class JobStatus
{
    private JobID jobid;
    private String jobidStr;
    private String userName;
    private String jobName;
    private int numMaps;
    private int numReduce;
    private int mapProgress;
    private int reduceProgress;
    private float cleanupProgress;
    private float setupProgress;
    private String runState;
    private long startTime;
    private JobPriority priority;
    private String schedulingInfo;
    private int completedMaps;
    private int completedReduces;
    
    public int getCompletedMaps() {
        return this.completedMaps;
    }
    
    public void setCompletedMaps(final int completedMaps) {
        this.completedMaps = completedMaps;
    }
    
    public int getCompletedReduces() {
        return this.completedReduces;
    }
    
    public void setCompletedReduces(final int completedReduces) {
        this.completedReduces = completedReduces;
    }
    
    public void init(final org.apache.hadoop.mapred.JobStatus job) {
        this.jobid = job.getJobID();
        this.jobidStr = job.getJobId();
        this.userName = job.getUsername();
        this.priority = job.getJobPriority();
        this.runState = org.apache.hadoop.mapred.JobStatus.getJobRunState(job.getRunState());
        this.schedulingInfo = job.getSchedulingInfo();
        this.startTime = job.getStartTime();
        this.mapProgress = (int)(job.mapProgress() * 100.0f);
        this.reduceProgress = (int)(job.reduceProgress() * 100.0f);
    }
    
    public String getJobidStr() {
        return this.jobidStr;
    }
    
    public void setJobidStr(final String jobidStr) {
        this.jobidStr = jobidStr;
    }
    
    public String getRunState() {
        return this.runState;
    }
    
    public void setRunState(final String runState) {
        this.runState = runState;
    }
    
    public JobID getJobid() {
        return this.jobid;
    }
    
    public void setJobid(final JobID jobid) {
        this.jobid = jobid;
    }
    
    public String getUserName() {
        return this.userName;
    }
    
    public void setUserName(final String userName) {
        this.userName = userName;
    }
    
    public String getJobName() {
        return this.jobName;
    }
    
    public void setJobName(final String jobName) {
        this.jobName = jobName;
    }
    
    public int getNumMaps() {
        return this.numMaps;
    }
    
    public void setNumMaps(final int numMaps) {
        this.numMaps = numMaps;
    }
    
    public int getNumReduce() {
        return this.numReduce;
    }
    
    public void setNumReduce(final int numReduce) {
        this.numReduce = numReduce;
    }
    
    public int getMapProgress() {
        return this.mapProgress;
    }
    
    public void setMapProgress(final int mapProgress) {
        this.mapProgress = mapProgress;
    }
    
    public int getReduceProgress() {
        return this.reduceProgress;
    }
    
    public void setReduceProgress(final int reduceProgress) {
        this.reduceProgress = reduceProgress;
    }
    
    public float getCleanupProgress() {
        return this.cleanupProgress;
    }
    
    public void setCleanupProgress(final float cleanupProgress) {
        this.cleanupProgress = cleanupProgress;
    }
    
    public float getSetupProgress() {
        return this.setupProgress;
    }
    
    public void setSetupProgress(final float setupProgress) {
        this.setupProgress = setupProgress;
    }
    
    public long getStartTime() {
        return this.startTime;
    }
    
    public void setStartTime(final long startTime) {
        this.startTime = startTime;
    }
    
    public JobPriority getPriority() {
        return this.priority;
    }
    
    public void setPriority(final JobPriority priority) {
        this.priority = priority;
    }
    
    public String getSchedulingInfo() {
        return this.schedulingInfo;
    }
    
    public void setSchedulingInfo(final String schedulingInfo) {
        this.schedulingInfo = schedulingInfo;
    }
}
