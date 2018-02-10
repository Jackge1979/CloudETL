package com.dataliance.hbase.impl;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.*;

import com.dataliance.hbase.*;
import com.dataliance.hbase.util.*;

import org.apache.hadoop.hbase.*;
import java.io.*;

import org.apache.hadoop.hbase.client.*;
import java.util.*;

public class HbaseCrudImpl implements HbaseCrud
{
    private HTableFactory htableFactory;
    private Configuration conf;
    
    public HbaseCrudImpl(final Configuration conf) {
        this(HTableFactory.getHTableFactory(conf));
    }
    
    public HbaseCrudImpl(final HTableFactory htableFactory) {
        this.htableFactory = htableFactory;
        this.conf = htableFactory.getConf();
    }
    
    @Override
    public void createTalbe(final String tableName, final String[] famliys) throws IOException {
        final HBaseAdmin admin = new HBaseAdmin(this.conf);
        final HTableDescriptor tableDescripter = new HTableDescriptor(Bytes.toBytes(tableName));
        for (final String fam : famliys) {
            tableDescripter.addFamily(new HColumnDescriptor(Bytes.toBytes(fam)));
        }
        admin.createTable(tableDescripter);
    }
    
    @Override
    public void dorpTable(final String tableName) throws IOException {
        final HBaseAdmin admin = new HBaseAdmin(this.conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }
    
    @Override
    public void insertRow(final String tableName, final String rowKey, final String famliy, final String qualifier, final Object value) throws IOException {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            final Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(famliy), Bytes.toBytes(qualifier), BaseTypeUtil.toBytes(value));
            htable.put(put);
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    @Override
    public void deleteRow(final String tableName, final String rowKey) throws IOException {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            final Delete delete = new Delete(Bytes.toBytes(rowKey));
            htable.delete(delete);
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    @Override
    public void deleteRow(final String tableName, final List<String> rowKeys) throws IOException {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            final List<Delete> deletes = new LinkedList<Delete>();
            for (final String rowKey : rowKeys) {
                deletes.add(new Delete(Bytes.toBytes(rowKey)));
            }
            htable.delete((List)deletes);
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    @Override
    public void deleteCell(final String tableName, final String rowKey, final String famliy, final String qualifier) throws IOException {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            final Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.deleteColumn(Bytes.toBytes(famliy), Bytes.toBytes(qualifier));
            htable.delete(delete);
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    @Override
    public void updateCell(final String tableName, final String rowKey, final String famliy, final String qualifier, final Object value) throws IOException {
        this.insertRow(tableName, rowKey, famliy, qualifier, value);
    }
    
    @Override
    public void deleteFamliy(final String tableName, final String famliy) throws IOException {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            final Delete delete = new Delete();
            delete.deleteFamily(Bytes.toBytes(famliy));
            htable.delete(delete);
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    @Override
    public void deleteFamliy(final String tableName, final List<String> famliys) throws IOException {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            final List<Delete> deletes = new LinkedList<Delete>();
            for (final String famliy : famliys) {
                final Delete delete = new Delete();
                delete.deleteFamily(Bytes.toBytes(famliy));
                deletes.add(delete);
            }
            htable.delete((List)deletes);
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
}
