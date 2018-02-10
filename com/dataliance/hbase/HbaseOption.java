package com.dataliance.hbase;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.client.*;

public abstract class HbaseOption extends Configured implements Configurable
{
    private HTableFactory htableFactroy;
    private String tableName;
    
    public HbaseOption(final Configuration conf, final String tableName) {
        super(conf);
        this.tableName = tableName;
        this.htableFactroy = HTableFactory.getHTableFactory(conf);
    }
    
    public HTableInterface getHTable() {
        return this.htableFactroy.getHTable(this.tableName);
    }
    
    public void release(final HTableInterface htable) {
        this.htableFactroy.release(htable);
    }
}
