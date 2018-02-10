package com.dataliance.etl.job.montior.impl;

import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.job.vo.*;
import com.dataliance.etl.job.montior.*;
import com.dataliance.etl.job.vo.*;

import java.util.*;
import java.io.*;

public class ProFinishMonitor implements ProgramMonitor, Writable
{
    private Host managerHost;
    private HostList dataHostList;
    private HostList importHostList;
    private DataList dataList;
    private DataList errDataList;
    private DataList finishDataList;
    private DataList runDataList;
    private DataList waitDataList;
    private FloatWritable complete;
    private JobInfo jobInfo;
    
    public ProFinishMonitor() {
        this.managerHost = new Host();
        this.dataHostList = new HostList();
        this.importHostList = new HostList();
        this.dataList = new DataList();
        this.errDataList = new DataList();
        this.finishDataList = new DataList();
        this.runDataList = new DataList();
        this.waitDataList = new DataList();
        this.complete = new FloatWritable(0.0f);
    }
    
    public ProFinishMonitor(final JobInfo jobInfo) {
        this.managerHost = new Host();
        this.dataHostList = new HostList();
        this.importHostList = new HostList();
        this.dataList = new DataList();
        this.errDataList = new DataList();
        this.finishDataList = new DataList();
        this.runDataList = new DataList();
        this.waitDataList = new DataList();
        this.complete = new FloatWritable(0.0f);
        this.jobInfo = jobInfo;
    }
    
    public ProFinishMonitor(final JobInfo jobInfo, final byte[] data) throws IOException {
        this(jobInfo, new ByteArrayInputStream(data));
    }
    
    public ProFinishMonitor(final JobInfo jobInfo, final InputStream input) throws IOException {
        this.managerHost = new Host();
        this.dataHostList = new HostList();
        this.importHostList = new HostList();
        this.dataList = new DataList();
        this.errDataList = new DataList();
        this.finishDataList = new DataList();
        this.runDataList = new DataList();
        this.waitDataList = new DataList();
        this.complete = new FloatWritable(0.0f);
        this.jobInfo = jobInfo;
        final DataInputStream in = new DataInputStream(input);
        this.readFields(in);
        input.close();
    }
    
    public ProFinishMonitor(final JobInfo jobInfo, final File file) throws IOException {
        this(jobInfo, new FileInputStream(file));
    }
    
    @Override
    public List<Host> getAllDataHosts() {
        return this.dataHostList.getHosts();
    }
    
    @Override
    public List<Host> getAllImportHosts() {
        return this.importHostList.getHosts();
    }
    
    @Override
    public List<Data> getAllDatas() {
        return this.dataList.getDatas();
    }
    
    @Override
    public List<Data> getAllFinishDatas() {
        return this.finishDataList.getDatas();
    }
    
    @Override
    public List<Data> getAllWaitDatas() {
        return this.waitDataList.getDatas();
    }
    
    @Override
    public List<Data> getAllRunDatas() {
        return this.runDataList.getDatas();
    }
    
    @Override
    public List<Data> getAllErrorDatas() {
        return this.errDataList.getDatas();
    }
    
    @Override
    public float getComplete() {
        return this.complete.get();
    }
    
    @Override
    public Host getManagerHost() {
        return this.managerHost;
    }
    
    public void write(final DataOutput out) throws IOException {
        this.managerHost.write(out);
        this.dataHostList.write(out);
        this.importHostList.write(out);
        this.dataList.write(out);
        this.errDataList.write(out);
        this.finishDataList.write(out);
        this.waitDataList.write(out);
        this.complete.write(out);
    }
    
    public void readFields(final DataInput in) throws IOException {
        this.managerHost.readFields(in);
        this.dataHostList.readFields(in);
        this.importHostList.readFields(in);
        this.dataList.readFields(in);
        this.errDataList.readFields(in);
        this.finishDataList.readFields(in);
        this.waitDataList.readFields(in);
        this.complete.readFields(in);
    }
    
    public void setManagerHost(final Host managerHost) {
        this.managerHost = managerHost;
    }
    
    public void setDataHostList(final HostList dataHostList) {
        this.dataHostList = dataHostList;
    }
    
    public void setImportHostList(final HostList importHostList) {
        this.importHostList = importHostList;
    }
    
    public void setDataList(final DataList dataList) {
        this.dataList = dataList;
    }
    
    public void setErrDataList(final DataList errDataList) {
        this.errDataList = errDataList;
    }
    
    public void setFinishDataList(final DataList finishDataList) {
        this.finishDataList = finishDataList;
    }
    
    public void setRunDataList(final DataList runDataList) {
        this.runDataList = runDataList;
    }
    
    public void setWaitDataList(final DataList waitDataList) {
        this.waitDataList = waitDataList;
    }
    
    public void setComplete(final FloatWritable complete) {
        this.complete = complete;
    }
    
    public JobInfo.JOB_TYPE getType() {
        return JobInfo.JOB_TYPE.PROGRAM;
    }
    
    public MapReduceMontior toMapReduceMontior() throws IOException {
        throw ProFinishMonitor.NOT_SUPPORT;
    }
    
    public ProgramMonitor toProgramMonitor() throws IOException {
        return this;
    }
    
    public String getJobName() {
        return this.jobInfo.getJobName();
    }
    
    public JobInfo.STATUS getStatus() {
        return this.jobInfo.getStatus();
    }
    
    public boolean isFinish() {
        return true;
    }
}
