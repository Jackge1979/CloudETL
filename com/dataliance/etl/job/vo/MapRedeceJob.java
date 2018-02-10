package com.dataliance.etl.job.vo;

public class MapRedeceJob
{
    private static final MapRedeceJob waitJob;
    private String Jobid;
    private String jobName;
    private String user;
    private String priority;
    private float mapComplete;
    private float reduceComplete;
    private State state;
    private long startTime;
    private long endTime;
    private boolean finish;
    
    public static final MapRedeceJob getWiatJob(final String jobName) {
        MapRedeceJob.waitJob.setJobName(jobName);
        return MapRedeceJob.waitJob;
    }
    
    public static final MapRedeceJob getError(final State state, final String jobName) {
        final MapRedeceJob errJob = new MapRedeceJob();
        errJob.setJobName(jobName);
        errJob.setState(state);
        errJob.setMapComplete(1.0f);
        errJob.setFinish(true);
        errJob.setReduceComplete(1.0f);
        return errJob;
    }
    
    public String getJobid() {
        return this.Jobid;
    }
    
    public void setJobid(final String jobid) {
        this.Jobid = jobid;
    }
    
    public String getJobName() {
        return this.jobName;
    }
    
    public void setJobName(final String jobName) {
        this.jobName = jobName;
    }
    
    public String getUser() {
        return this.user;
    }
    
    public void setUser(final String user) {
        this.user = user;
    }
    
    public String getPriority() {
        return this.priority;
    }
    
    public void setPriority(final String priority) {
        this.priority = priority;
    }
    
    public float getMapComplete() {
        return this.mapComplete;
    }
    
    public void setMapComplete(final float mapComplete) {
        this.mapComplete = mapComplete;
    }
    
    public float getReduceComplete() {
        return this.reduceComplete;
    }
    
    public void setReduceComplete(final float reduceComplete) {
        this.reduceComplete = reduceComplete;
    }
    
    public State getState() {
        return this.state;
    }
    
    public void setState(final State state) {
        this.state = state;
    }
    
    public long getStartTime() {
        return this.startTime;
    }
    
    public void setStartTime(final long startTime) {
        this.startTime = startTime;
    }
    
    public long getEndTime() {
        return this.endTime;
    }
    
    public void setEndTime(final long endTime) {
        this.endTime = endTime;
    }
    
    public boolean isFinish() {
        return this.finish;
    }
    
    public void setFinish(final boolean finish) {
        this.finish = finish;
    }
    
    static {
        (waitJob = new MapRedeceJob()).setState(State.WIATING);
    }
    
    public enum State
    {
        UNKNOWN(1), 
        RUNNING(2), 
        SUCCEEDED(3), 
        FAILED(4), 
        PREP(5), 
        KILLED(6), 
        WIATING(7);
        
        int value;
        
        private State(final int value) {
            this.value = value;
        }
        
        public int getValue() {
            return this.value;
        }
    }
}
