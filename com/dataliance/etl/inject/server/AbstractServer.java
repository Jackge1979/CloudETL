package com.dataliance.etl.inject.server;

import org.apache.hadoop.conf.*;

public abstract class AbstractServer extends Configured implements Configurable
{
    public AbstractServer() {
    }
    
    public AbstractServer(final Configuration conf) {
        super(conf);
    }
}
