package com.dataliance.hadoop.mapred.vo;

import org.apache.hadoop.mapred.*;
import java.net.*;

public class JobProfile
{
    private String user;
    private JobID jobid;
    private String jobFile;
    private URL url;
    private String name;
    private String submitHost;
    private String submitHostAddr;
    private String jobSetup;
    private String status;
    private long startTime;
    private long finishedTime;
    private long totalTime;
    private String jobCleanup;
    private float mapprocess;
    private float reduceProcess;
    private int mapTasks;
    private int reduceTasks;
    private int mapPending;
    private int reducePending;
    private int mapRunning;
    private int reduceRunning;
    private int mapComplete;
    private int reduceComplete;
    private int mapKill;
    private int reduceKill;
    
    public String getUser() {
        return this.user;
    }
    
    public void setUser(final String user) {
        this.user = user;
    }
    
    public JobID getJobid() {
        return this.jobid;
    }
    
    public void setJobid(final JobID jobid) {
        this.jobid = jobid;
    }
    
    public String getJobFile() {
        return this.jobFile;
    }
    
    public void setJobFile(final String jobFile) {
        this.jobFile = jobFile;
    }
    
    public URL getUrl() {
        return this.url;
    }
    
    public void setUrl(final URL url) {
        this.url = url;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public long getStartTime() {
        return this.startTime;
    }
    
    public void setStartTime(final long startTime) {
        this.startTime = startTime;
    }
    
    public long getFinishedTime() {
        return this.finishedTime;
    }
    
    public void setFinishedTime(final long finishedTime) {
        this.finishedTime = finishedTime;
    }
    
    public long getTotalTime() {
        return this.totalTime;
    }
    
    public void setTotalTime(final long totalTime) {
        this.totalTime = totalTime;
    }
    
    public String getSubmitHost() {
        return this.submitHost;
    }
    
    public void setSubmitHost(final String submitHost) {
        this.submitHost = submitHost;
    }
    
    public String getSubmitHostAddr() {
        return this.submitHostAddr;
    }
    
    public void setSubmitHostAddr(final String submitHostAddr) {
        this.submitHostAddr = submitHostAddr;
    }
    
    public String getJobSetup() {
        return this.jobSetup;
    }
    
    public void setJobSetup(final String jobSetup) {
        this.jobSetup = jobSetup;
    }
    
    public String getStatus() {
        return this.status;
    }
    
    public void setStatus(final String status) {
        this.status = status;
    }
    
    public String getJobCleanup() {
        return this.jobCleanup;
    }
    
    public void setJobCleanup(final String jobCleanup) {
        this.jobCleanup = jobCleanup;
    }
    
    public float getMapprocess() {
        return this.mapprocess;
    }
    
    public void setMapprocess(final float mapprocess) {
        this.mapprocess = mapprocess;
    }
    
    public float getReduceProcess() {
        return this.reduceProcess;
    }
    
    public void setReduceProcess(final float reduceProcess) {
        this.reduceProcess = reduceProcess;
    }
    
    public int getMapTasks() {
        return this.mapTasks;
    }
    
    public void setMapTasks(final int mapTasks) {
        this.mapTasks = mapTasks;
    }
    
    public int getReduceTasks() {
        return this.reduceTasks;
    }
    
    public void setReduceTasks(final int reduceTasks) {
        this.reduceTasks = reduceTasks;
    }
    
    public int getMapPending() {
        return this.mapPending;
    }
    
    public void setMapPending(final int mapPending) {
        this.mapPending = mapPending;
    }
    
    public int getReducePending() {
        return this.reducePending;
    }
    
    public void setReducePending(final int reducePending) {
        this.reducePending = reducePending;
    }
    
    public int getMapRunning() {
        return this.mapRunning;
    }
    
    public void setMapRunning(final int mapRunning) {
        this.mapRunning = mapRunning;
    }
    
    public int getReduceRunning() {
        return this.reduceRunning;
    }
    
    public void setReduceRunning(final int reduceRunning) {
        this.reduceRunning = reduceRunning;
    }
    
    public int getMapComplete() {
        return this.mapComplete;
    }
    
    public void setMapComplete(final int mapComplete) {
        this.mapComplete = mapComplete;
    }
    
    public int getReduceComplete() {
        return this.reduceComplete;
    }
    
    public void setReduceComplete(final int reduceComplete) {
        this.reduceComplete = reduceComplete;
    }
    
    public int getMapKill() {
        return this.mapKill;
    }
    
    public void setMapKill(final int mapKill) {
        this.mapKill = mapKill;
    }
    
    public int getReduceKill() {
        return this.reduceKill;
    }
    
    public void setReduceKill(final int reduceKill) {
        this.reduceKill = reduceKill;
    }
}
