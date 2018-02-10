package com.dataliance.hbase.insert;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.client.*;
import java.io.*;
import org.apache.hadoop.hbase.util.*;

import com.dataliance.hbase.*;

import java.util.*;

public abstract class HbaseInsert<V> extends HbaseOption
{
    public HbaseInsert(final Configuration conf, final String tableName) {
        super(conf, tableName);
    }
    
    public void insert(final V v) throws IOException {
        final Put put = this.parse(v);
        final HTableInterface htable = this.getHTable();
        try {
            htable.put(put);
        }
        finally {
            this.release(htable);
        }
    }
    
    public long increment(final String rowKey, final String famliy, final String qualifier, final long increment) throws IOException {
        return this.increment(Bytes.toBytes(rowKey), Bytes.toBytes(famliy), Bytes.toBytes(qualifier), increment);
    }
    
    public long increment(final String rowKey, final byte[] famliy, final byte[] qualifier, final long increment) throws IOException {
        return this.increment(Bytes.toBytes(rowKey), famliy, qualifier, increment);
    }
    
    public long increment(final byte[] rowKey, final byte[] famliy, final byte[] qualifier, final long increment) throws IOException {
        final HTableInterface htable = this.getHTable();
        try {
            return htable.incrementColumnValue(rowKey, famliy, qualifier, increment);
        }
        finally {
            this.release(htable);
        }
    }
    
    public void insert(final List<V> vs) throws IOException {
        final List<Put> puts = new ArrayList<Put>();
        for (final V v : vs) {
            puts.add(this.parse(v));
        }
        final HTableInterface htable = this.getHTable();
        try {
            htable.put((List)puts);
        }
        finally {
            this.release(htable);
        }
    }
    
    protected abstract Put parse(final V p0) throws IOException;
}
