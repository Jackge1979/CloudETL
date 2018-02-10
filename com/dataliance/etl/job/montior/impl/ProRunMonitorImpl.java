package com.dataliance.etl.job.montior.impl;

import org.apache.hadoop.conf.*;
import java.net.*;
import org.apache.hadoop.ipc.*;

import com.dataliance.etl.inject.job.vo.*;
import com.dataliance.etl.inject.rpc.*;
import com.dataliance.etl.job.montior.*;
import com.dataliance.etl.job.vo.*;

import java.io.*;
import java.util.*;

public class ProRunMonitorImpl extends Configured implements ProgramMonitor
{
    private JobClient client;
    private String jobName;
    
    public ProRunMonitorImpl(final String jobName, final Configuration conf, final String host, final int port) throws IOException {
        super(conf);
        this.client = (JobClient)RPC.getProxy((Class)JobClient.class, 1L, new InetSocketAddress(host, port), conf);
    }
    
    public List<Host> getAllDataHosts() {
        return this.client.getAllDataHosts().getHosts();
    }
    
    public List<Host> getAllImportHosts() {
        return this.client.getAllImportHosts().getHosts();
    }
    
    public List<Data> getAllDatas() {
        return this.client.getAllDatas().getDatas();
    }
    
    public List<Data> getAllFinishDatas() {
        return this.client.getAllFinishDatas().getDatas();
    }
    
    public List<Data> getAllWaitDatas() {
        return this.client.getAllWaitDatas().getDatas();
    }
    
    public List<Data> getAllRunDatas() {
        return this.client.getAllRunDatas().getDatas();
    }
    
    public List<Data> getAllErrorDatas() {
        return this.client.getAllErrorDatas().getDatas();
    }
    
    public float getComplete() {
        return this.client.getComplete().get();
    }
    
    public Host getManagerHost() {
        return this.client.getManagerHost();
    }
    
    public JobInfo.JOB_TYPE getType() {
        return JobInfo.JOB_TYPE.PROGRAM;
    }
    
    public MapReduceMontior toMapReduceMontior() throws IOException {
        throw ProRunMonitorImpl.NOT_SUPPORT;
    }
    
    public ProgramMonitor toProgramMonitor() throws IOException {
        return this;
    }
    
    public void setJobName(final String jobName) {
        this.jobName = jobName;
    }
    
    public String getJobName() {
        return this.jobName;
    }
    
    public JobInfo.STATUS getStatus() {
        return JobInfo.STATUS.RUNNING;
    }
    
    public boolean isFinish() {
        return false;
    }
}
