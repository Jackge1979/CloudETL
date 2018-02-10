package com.dataliance.etl.inject.rpc;

import java.io.*;
import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.etl.vo.*;

public interface ImportClient extends Client
{
    public static final long versionID = 1L;
    
    ImportVO regist(final Text p0) throws IOException;
    
    void finish(final Text p0, final ImportVO p1) throws IOException;
    
    void error(final Text p0, final ImportVO p1) throws IOException;
    
    ImportVO next(final Text p0) throws IOException;
    
    void report(final Text p0, final Text p1, final FloatWritable p2);
}
