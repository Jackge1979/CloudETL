package com.dataliance.etl.inject.rpc;

import java.io.*;

public interface Manager extends ImportClient, SourceClient, TaskClient, JobClient
{
    public static final long versionID = 1L;
    
    void close() throws IOException;
    
    void start() throws IOException;
    
    void deployDataServer() throws IOException;
    
    void startDataServer() throws IOException;
    
    void deployImportServer() throws IOException;
    
    void startImportServer() throws IOException;
}
