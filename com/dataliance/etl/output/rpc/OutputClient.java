package com.dataliance.etl.output.rpc;

import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.rpc.*;
import com.dataliance.etl.output.vo.*;

import java.io.*;

public interface OutputClient extends Client
{
    public static final long versionID = 1L;
    
    OutputVO regist(final Text p0) throws IOException;
    
    void finish(final Text p0, final OutputVO p1) throws IOException;
    
    void error(final Text p0, final OutputVO p1) throws IOException;
    
    OutputVO next(final Text p0) throws IOException;
    
    void report(final Text p0, final Text p1);
}
