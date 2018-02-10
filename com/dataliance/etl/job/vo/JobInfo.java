package com.dataliance.etl.job.vo;

import java.util.*;
import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.util.*;

import java.io.*;

public class JobInfo implements Writable
{
    private static final byte VERSION = 1;
    private String programID;
    private String host;
    private String jobID;
    private String jobName;
    private String taskName;
    private int port;
    private long startTime;
    private long endTime;
    private JOB_TYPE type;
    private STATUS status;
    private byte[] jobData;
    private int index;
    
    public JobInfo() {
        this.jobData = new byte[0];
    }
    
    public static void main(final String[] args) {
        System.out.println(new Random().nextInt(1));
    }
    
    public String getHost() {
        return this.host;
    }
    
    public void setHost(final String host) {
        this.host = host;
    }
    
    public int getPort() {
        return this.port;
    }
    
    public void setPort(final int port) {
        this.port = port;
    }
    
    public String getJobName() {
        return this.jobName;
    }
    
    public void setJobName(final String jobName) {
        this.jobName = jobName;
    }
    
    public String getTaskName() {
        return this.taskName;
    }
    
    public void setTaskName(final String taskName) {
        this.taskName = taskName;
    }
    
    public byte[] getJobData() {
        return this.jobData;
    }
    
    public void setJobData(final byte[] jobData) {
        this.jobData = jobData;
    }
    
    public String getProgramID() {
        return this.programID;
    }
    
    public void setProgramID(final String programID) {
        this.programID = programID;
    }
    
    public String getJobID() {
        return this.jobID;
    }
    
    public STATUS getStatus() {
        return this.status;
    }
    
    public void setStatus(final STATUS status) {
        this.status = status;
    }
    
    public void setJobID(final String jobID) {
        this.jobID = jobID;
    }
    
    public JOB_TYPE getType() {
        return this.type;
    }
    
    public long getEndTime() {
        return this.endTime;
    }
    
    public void setEndTime(final long endTime) {
        this.endTime = endTime;
    }
    
    public void setType(final JOB_TYPE type) {
        this.type = type;
    }
    
    public long getStartTime() {
        return this.startTime;
    }
    
    public void setStartTime(final long startTime) {
        this.startTime = startTime;
    }
    
    public int getIndex() {
        return this.index;
    }
    
    public void setIndex(final int index) {
        this.index = index;
    }
    
    public void write(final DataOutput out) throws IOException {
        out.writeByte(1);
        WritableUtils.writeString(out, this.programID);
        WritableUtils.writeString(out, this.host);
        WritableUtils.writeString(out, this.jobID);
        WritableUtils.writeString(out, this.jobName);
        WritableUtils.writeString(out, this.taskName);
        WritableUtils.writeVInt(out, this.port);
        WritableUtils.writeVInt(out, this.index);
        WritableUtils.writeVLong(out, this.startTime);
        WritableUtils.writeVLong(out, this.endTime);
        WritableUtils.writeEnum(out, (Enum)this.type);
        WritableUtils.writeEnum(out, (Enum)this.status);
        WritableUtils.writeVInt(out, this.jobData.length);
        out.write(this.jobData);
    }
    
    public void readFields(final DataInput in) throws IOException {
        final byte v = in.readByte();
        if (v == 1) {
            this.programID = WritableUtils.readString(in);
            this.host = WritableUtils.readString(in);
            this.jobID = WritableUtils.readString(in);
            this.jobName = WritableUtils.readString(in);
            this.taskName = WritableUtils.readString(in);
            this.port = WritableUtils.readVInt(in);
            this.index = WritableUtils.readVInt(in);
            this.startTime = WritableUtils.readVLong(in);
            this.endTime = WritableUtils.readVLong(in);
            this.type = (JOB_TYPE)WritableUtils.readEnum(in, (Class)JOB_TYPE.class);
            this.status = (STATUS)WritableUtils.readEnum(in, (Class)STATUS.class);
            final int size = WritableUtils.readVInt(in);
            in.readFully(this.jobData = new byte[size]);
            return;
        }
        throw new VersionException(v, (byte)1);
    }
    
    public enum JOB_TYPE
    {
        PROGRAM, 
        MAPREDUCE;
    }
    
    public enum STATUS
    {
        RUNNING, 
        KILLED, 
        FINISH, 
        ERROR;
    }
}
