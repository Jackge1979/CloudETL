package com.dataliance.hadoop.hdfs.vo;

public class DatanodeInfo
{
    private String name;
    private String capacity;
    private String dfsUsed;
    private String nonDFSUsed;
    private String remaining;
    private String dfsUsedPercent;
    private String remainingPercent;
    private String lastContact;
    private int xceiverCount;
    private String hostName;
    private String adminState;
    private String host;
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public String getCapacity() {
        return this.capacity;
    }
    
    public void setCapacity(final String capacity) {
        this.capacity = capacity;
    }
    
    public String getDfsUsed() {
        return this.dfsUsed;
    }
    
    public void setDfsUsed(final String dfsUsed) {
        this.dfsUsed = dfsUsed;
    }
    
    public String getNonDFSUsed() {
        return this.nonDFSUsed;
    }
    
    public void setNonDFSUsed(final String nonDFSUsed) {
        this.nonDFSUsed = nonDFSUsed;
    }
    
    public String getRemaining() {
        return this.remaining;
    }
    
    public void setRemaining(final String remaining) {
        this.remaining = remaining;
    }
    
    public String getDfsUsedPercent() {
        return this.dfsUsedPercent;
    }
    
    public void setDfsUsedPercent(final String dfsUsedPercent) {
        this.dfsUsedPercent = dfsUsedPercent;
    }
    
    public String getRemainingPercent() {
        return this.remainingPercent;
    }
    
    public void setRemainingPercent(final String remainingPercent) {
        this.remainingPercent = remainingPercent;
    }
    
    public String getLastContact() {
        return this.lastContact;
    }
    
    public void setLastContact(final String lastContact) {
        this.lastContact = lastContact;
    }
    
    public int getXceiverCount() {
        return this.xceiverCount;
    }
    
    public void setXceiverCount(final int xceiverCount) {
        this.xceiverCount = xceiverCount;
    }
    
    public String getHostName() {
        return this.hostName;
    }
    
    public void setHostName(final String hostName) {
        this.hostName = hostName;
    }
    
    public String getAdminState() {
        return this.adminState;
    }
    
    public void setAdminState(final String adminState) {
        this.adminState = adminState;
    }
    
    public String getHost() {
        return this.host;
    }
    
    public void setHost(final String host) {
        this.host = host;
    }
}
