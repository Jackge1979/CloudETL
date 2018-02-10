package com.dataliance.etl.io;

import java.io.*;
import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.rpc.*;

public interface IOManager extends Closeable, Client
{
    public static final long versionID = 1L;
    
    void start() throws IOException;
    
    void report(final Text p0, final Text p1);
}
