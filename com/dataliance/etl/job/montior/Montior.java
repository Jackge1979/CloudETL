package com.dataliance.etl.job.montior;

import java.io.*;

import com.dataliance.etl.job.vo.*;

public interface Montior
{
    public static final IOException NOT_SUPPORT = new IOException("not support");
    
    JobInfo.JOB_TYPE getType();
    
    MapReduceMontior toMapReduceMontior() throws IOException;
    
    ProgramMonitor toProgramMonitor() throws IOException;
    
    JobInfo.STATUS getStatus();
    
    String getJobName();
    
    boolean isFinish();
}
