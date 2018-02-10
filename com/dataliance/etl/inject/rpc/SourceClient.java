package com.dataliance.etl.inject.rpc;

import java.io.*;
import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.etl.vo.*;
import com.dataliance.etl.inject.etl.vo.http.*;

public interface SourceClient extends Client
{
    public static final long versionID = 1L;
    
    void list(final Text p0, final HttpDataSource p1) throws IOException;
    
    DataHost regist(final Text p0, final IntWritable p1);
    
    void finish(final Text p0);
    
    void log(final Text p0, final Text p1);
}
