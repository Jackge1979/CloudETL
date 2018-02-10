package com.dataliance.hbase.reader.impl;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.*;

import com.dataliance.hbase.*;
import com.dataliance.hbase.reader.*;
import com.dataliance.hbase.util.*;
import com.dataliance.util.*;

import java.io.*;
import org.apache.hadoop.hbase.client.*;
import java.util.*;

public class HbaseGetterImpl implements HbaseGetter
{
    private HTableFactory htFactory;
    
    public HbaseGetterImpl(final HTableFactory htFactory) {
        this.htFactory = htFactory;
    }
    
    public HbaseGetterImpl(final Configuration conf) {
        this(HTableFactory.getHTableFactory(conf));
    }
    
    @Override
    public Object getValue(final String tableName, final String rowKey, final String family, final String qualifier, final Class reType) {
        final HTableInterface htable = this.htFactory.getHTable(tableName);
        try {
            final Get get = new Get(Bytes.toBytes(rowKey));
            final Result result = htable.get(get);
            final byte[] b = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            if (b == null) {
                return null;
            }
            return BaseTypeUtil.getValue(reType, b);
        }
        catch (IOException e) {
            LogUtil.error(this.getClass(), e.getMessage());
            final Result result = null;
            return result;
        }
        finally {
            this.htFactory.release(htable);
        }
    }
    
    @Override
    public String getValue(final String tableName, final String rowKey, final String family, final String qualifier) {
        return (String)this.getValue(tableName, rowKey, family, qualifier, String.class);
    }
    
    @Override
    public void putValue(final String tableName, final String rowKey, final String family, final String qualifier, final byte[] value) {
        final Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), value);
        this.pubValue(tableName, put);
    }
    
    @Override
    public void pubValue(final String tableName, final Put put) {
        final HTableInterface htable = this.htFactory.getHTable(tableName);
        try {
            htable.put(put);
        }
        catch (IOException e) {
            LogUtil.error(this.getClass(), e.getMessage());
        }
        finally {
            this.htFactory.release(htable);
        }
    }
    
    @Override
    public void putValue(final String tableName, final List<Put> puts) {
        final HTableInterface htable = this.htFactory.getHTable(tableName);
        try {
            htable.put((List)puts);
        }
        catch (IOException e) {
            LogUtil.error(this.getClass(), e.getMessage());
        }
        finally {
            this.htFactory.release(htable);
        }
    }
}
