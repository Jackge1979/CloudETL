package com.dataliance.etl.inject.rpc;

import com.dataliance.etl.inject.etl.vo.*;

public interface TaskClient extends Client
{
    public static final long VERSION = 1L;
    
    void addDataHost(final Host p0);
    
    void addImportHost(final Host p0);
    
    void commit() throws Exception;
}
