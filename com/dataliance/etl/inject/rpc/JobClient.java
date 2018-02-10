package com.dataliance.etl.inject.rpc;

import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.job.vo.*;

public interface JobClient extends Client
{
    public static final long versionID = 1L;
    
    Host getManagerHost();
    
    HostList getAllDataHosts();
    
    HostList getAllImportHosts();
    
    DataList getAllDatas();
    
    DataList getAllFinishDatas();
    
    DataList getAllWaitDatas();
    
    DataList getAllRunDatas();
    
    DataList getAllErrorDatas();
    
    FloatWritable getComplete();
}
