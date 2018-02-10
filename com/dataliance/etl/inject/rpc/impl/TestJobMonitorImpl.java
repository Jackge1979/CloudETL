package com.dataliance.etl.inject.rpc.impl;

import org.apache.hadoop.conf.*;

import com.dataliance.etl.inject.job.vo.*;
import com.dataliance.etl.job.montior.*;
import com.dataliance.etl.job.vo.*;

import java.io.*;
import java.util.*;

public class TestJobMonitorImpl extends Configured implements ProgramMonitor
{
    private static final Random ran;
    
    public TestJobMonitorImpl() {
    }
    
    public TestJobMonitorImpl(final Configuration conf, final String host, final int port) throws IOException {
    }
    
    public List<Host> getAllDataHosts() {
        final List<Host> hosts = new ArrayList<Host>();
        for (int i = 0; i < TestJobMonitorImpl.ran.nextInt(20); ++i) {
            final Host host = new Host();
            host.setHost("datanode" + (i + 1));
            host.setHostType((byte)0);
            host.setPort(TestJobMonitorImpl.ran.nextInt(65535) + 1);
            for (int j = 0; j < TestJobMonitorImpl.ran.nextInt(50); ++j) {
                final Data data = new Data();
                data.setDataHost(host.getHost());
                data.setPath("/opt/import_test/" + i + "_" + j + ".txt");
                data.setDataSize(TestJobMonitorImpl.ran.nextLong());
                data.setStatus((byte)TestJobMonitorImpl.ran.nextInt(3));
                host.addData(data);
                if (data.getStatus() != 0) {
                    data.addImportHost("importdata" + TestJobMonitorImpl.ran.nextInt(10));
                }
            }
            hosts.add(host);
        }
        return hosts;
    }
    
    public List<Host> getAllImportHosts() {
        final List<Host> hosts = new ArrayList<Host>();
        for (int i = 0; i < TestJobMonitorImpl.ran.nextInt(20); ++i) {
            final Host host = new Host();
            host.setHost("importnode" + (i + 1));
            host.setHostType((byte)0);
            for (int j = 0; j < TestJobMonitorImpl.ran.nextInt(50); ++j) {
                final Data data = new Data();
                data.setDataHost(host.getHost());
                data.setPath("/opt/import_test/" + i + "_" + j + ".txt");
                data.setDataSize(TestJobMonitorImpl.ran.nextLong());
                data.setStatus((byte)TestJobMonitorImpl.ran.nextInt(3));
                if (data.getStatus() != 0) {
                    data.addImportHost("importdata" + (i + 1));
                }
                host.addData(data);
            }
            hosts.add(host);
        }
        return hosts;
    }
    
    public List<Data> getAllDatas() {
        final List<Data> datas = new ArrayList<Data>();
        for (int j = 0; j < TestJobMonitorImpl.ran.nextInt(50); ++j) {
            final Data data = new Data();
            data.setDataHost("datanode" + TestJobMonitorImpl.ran.nextInt(10));
            data.setPath("/opt/import_test/" + TestJobMonitorImpl.ran.nextInt(10) + "_" + TestJobMonitorImpl.ran.nextInt(10) + ".txt");
            data.setDataSize(TestJobMonitorImpl.ran.nextLong());
            data.setStatus((byte)TestJobMonitorImpl.ran.nextInt(3));
            if (data.getStatus() != 0) {
                data.addImportHost("importdata" + TestJobMonitorImpl.ran.nextInt(10));
            }
            datas.add(data);
        }
        return datas;
    }
    
    public List<Data> getAllFinishDatas() {
        final List<Data> datas = new ArrayList<Data>();
        for (int j = 0; j < TestJobMonitorImpl.ran.nextInt(50); ++j) {
            final Data data = new Data();
            data.setDataHost("datanode" + TestJobMonitorImpl.ran.nextInt(10));
            data.setPath("/opt/import_test/" + TestJobMonitorImpl.ran.nextInt(10) + "_" + TestJobMonitorImpl.ran.nextInt(10) + ".txt");
            data.setDataSize(TestJobMonitorImpl.ran.nextLong());
            data.setStatus((byte)2);
            if (data.getStatus() != 0) {
                data.addImportHost("importdata" + TestJobMonitorImpl.ran.nextInt(10));
            }
            datas.add(data);
        }
        return datas;
    }
    
    public List<Data> getAllWaitDatas() {
        final List<Data> datas = new ArrayList<Data>();
        for (int j = 0; j < TestJobMonitorImpl.ran.nextInt(50); ++j) {
            final Data data = new Data();
            data.setDataHost("datanode" + TestJobMonitorImpl.ran.nextInt(10));
            data.setPath("/opt/import_test/" + TestJobMonitorImpl.ran.nextInt(10) + "_" + TestJobMonitorImpl.ran.nextInt(10) + ".txt");
            data.setDataSize(TestJobMonitorImpl.ran.nextLong());
            data.setStatus((byte)0);
            if (data.getStatus() != 0) {
                data.addImportHost("importdata" + TestJobMonitorImpl.ran.nextInt(10));
            }
            datas.add(data);
        }
        return datas;
    }
    
    public List<Data> getAllRunDatas() {
        final List<Data> datas = new ArrayList<Data>();
        for (int j = 0; j < TestJobMonitorImpl.ran.nextInt(50); ++j) {
            final Data data = new Data();
            data.setDataHost("datanode" + TestJobMonitorImpl.ran.nextInt(10));
            data.setPath("/opt/import_test/" + TestJobMonitorImpl.ran.nextInt(10) + "_" + TestJobMonitorImpl.ran.nextInt(10) + ".txt");
            data.setDataSize(TestJobMonitorImpl.ran.nextLong());
            data.setStatus((byte)1);
            if (data.getStatus() != 0) {
                data.addImportHost("importdata" + TestJobMonitorImpl.ran.nextInt(10));
            }
            datas.add(data);
        }
        return datas;
    }
    
    public List<Data> getAllErrorDatas() {
        final List<Data> datas = new ArrayList<Data>();
        for (int j = 0; j < TestJobMonitorImpl.ran.nextInt(50); ++j) {
            final Data data = new Data();
            data.setDataHost("datanode" + TestJobMonitorImpl.ran.nextInt(10));
            data.setPath("/opt/import_test/" + TestJobMonitorImpl.ran.nextInt(10) + "_" + TestJobMonitorImpl.ran.nextInt(10) + ".txt");
            data.setDataSize(TestJobMonitorImpl.ran.nextLong());
            data.setStatus((byte)(-1));
            if (data.getStatus() != 0) {
                data.addImportHost("importdata" + TestJobMonitorImpl.ran.nextInt(10));
            }
            datas.add(data);
        }
        return datas;
    }
    
    public float getComplete() {
        return TestJobMonitorImpl.ran.nextInt(100) / 100;
    }
    
    public Host getManagerHost() {
        final Host host = new Host();
        host.setHost("master");
        host.setHostType((byte)(-1));
        host.setPort(TestJobMonitorImpl.ran.nextInt(65535) + 1);
        return host;
    }
    
    public JobInfo.JOB_TYPE getType() {
        return JobInfo.JOB_TYPE.PROGRAM;
    }
    
    public MapReduceMontior toMapReduceMontior() throws IOException {
        throw TestJobMonitorImpl.NOT_SUPPORT;
    }
    
    public ProgramMonitor toProgramMonitor() throws IOException {
        return this;
    }
    
    public String getJobName() {
        return "test-job";
    }
    
    public JobInfo.STATUS getStatus() {
        return JobInfo.STATUS.RUNNING;
    }
    
    public boolean isFinish() {
        return false;
    }
    
    static {
        ran = new Random();
    }
}
