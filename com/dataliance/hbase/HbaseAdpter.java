package com.dataliance.hbase;

import org.apache.hadoop.conf.*;

import com.dataliance.hbase.delete.*;
import com.dataliance.hbase.insert.*;

import java.io.*;
import java.util.*;

public abstract class HbaseAdpter<V> extends HbaseInsert<V>
{
    private HbaseDelete delete;
    
    public HbaseAdpter(final Configuration conf, final String tableName) {
        super(conf, tableName);
        this.delete = new HbaseDelete(conf, tableName);
    }
    
    public void delete(final byte[] rowKey) throws IOException {
        this.delete.delete(rowKey);
    }
    
    public void delete(final String rowKey) throws IOException {
        this.delete.delete(rowKey);
    }
    
    public void delete(final List<String> rows) throws IOException {
        this.delete.delete(rows);
    }
}
