package com.dataliance.hadoop.mapred.vo;

import java.util.*;

public class HistoryJobProfile
{
    private String user;
    private String jobid;
    private String jobFile;
    private String name;
    private String status;
    private String submitTime;
    private String startTime;
    private String finishedTime;
    private Map setup;
    private Map map;
    private Map reduce;
    private Map cleanup;
    
    public String getUser() {
        return this.user;
    }
    
    public void setUser(final String user) {
        this.user = user;
    }
    
    public String getJobFile() {
        return this.jobFile;
    }
    
    public void setJobFile(final String jobFile) {
        this.jobFile = jobFile;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public String getStatus() {
        return this.status;
    }
    
    public void setStatus(final String status) {
        this.status = status;
    }
    
    public String getSubmitTime() {
        return this.submitTime;
    }
    
    public void setSubmitTime(final String submitTime) {
        this.submitTime = submitTime;
    }
    
    public String getStartTime() {
        return this.startTime;
    }
    
    public void setStartTime(final String startTime) {
        this.startTime = startTime;
    }
    
    public String getFinishedTime() {
        return this.finishedTime;
    }
    
    public void setFinishedTime(final String finishedTime) {
        this.finishedTime = finishedTime;
    }
    
    public Map getSetup() {
        return this.setup;
    }
    
    public void setSetup(final Map setup) {
        this.setup = setup;
    }
    
    public Map getMap() {
        return this.map;
    }
    
    public void setMap(final Map map) {
        this.map = map;
    }
    
    public Map getReduce() {
        return this.reduce;
    }
    
    public String getJobid() {
        return this.jobid;
    }
    
    public void setJobid(final String jobid) {
        this.jobid = jobid;
    }
    
    public void setReduce(final Map reduce) {
        this.reduce = reduce;
    }
    
    public Map getCleanup() {
        return this.cleanup;
    }
    
    public void setCleanup(final Map cleanup) {
        this.cleanup = cleanup;
    }
}
