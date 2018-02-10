package com.dataliance.hadoop.mapred.vo;

public class ClusterStatus
{
    private long usedMemory;
    private long maxMemory;
    private int runningMapTasks;
    private int runningReduceTasks;
    private int totalSumit;
    private int nodes;
    private int mapTaskCapacity;
    private int reduceTaskCapacity;
    private int blacklistNodes;
    private int excludedNodes;
    
    public long getUsedMemory() {
        return this.usedMemory;
    }
    
    public void setUsedMemory(final long usedMemory) {
        this.usedMemory = usedMemory;
    }
    
    public long getMaxMemory() {
        return this.maxMemory;
    }
    
    public void setMaxMemory(final long maxMemory) {
        this.maxMemory = maxMemory;
    }
    
    public int getRunningMapTasks() {
        return this.runningMapTasks;
    }
    
    public void setRunningMapTasks(final int runningMapTasks) {
        this.runningMapTasks = runningMapTasks;
    }
    
    public int getRunningReduceTasks() {
        return this.runningReduceTasks;
    }
    
    public void setRunningReduceTasks(final int runningReduceTasks) {
        this.runningReduceTasks = runningReduceTasks;
    }
    
    public int getTotalSumit() {
        return this.totalSumit;
    }
    
    public void setTotalSumit(final int totalSumit) {
        this.totalSumit = totalSumit;
    }
    
    public int getNodes() {
        return this.nodes;
    }
    
    public void setNodes(final int nodes) {
        this.nodes = nodes;
    }
    
    public int getMapTaskCapacity() {
        return this.mapTaskCapacity;
    }
    
    public void setMapTaskCapacity(final int mapTaskCapacity) {
        this.mapTaskCapacity = mapTaskCapacity;
    }
    
    public int getReduceTaskCapacity() {
        return this.reduceTaskCapacity;
    }
    
    public void setReduceTaskCapacity(final int reduceTaskCapacity) {
        this.reduceTaskCapacity = reduceTaskCapacity;
    }
    
    public int getBlacklistNodes() {
        return this.blacklistNodes;
    }
    
    public void setBlacklistNodes(final int blacklistNodes) {
        this.blacklistNodes = blacklistNodes;
    }
    
    public int getExcludedNodes() {
        return this.excludedNodes;
    }
    
    public void setExcludedNodes(final int excludedNodes) {
        this.excludedNodes = excludedNodes;
    }
}
