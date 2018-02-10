package com.dataliance.hbase;

import org.apache.hadoop.conf.*;
import java.io.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.regionserver.*;

public class HTableFactory implements Serializable
{
    private static final long serialVersionUID = 1L;
    private static HTableFactory htableFactory;
    private static final int MAXCONN = 1000;
    private Configuration conf;
    private HTablePool htablePool;
    
    private HTableFactory(final Configuration conf) {
        this(conf, 1000);
    }
    
    private HTableFactory(final Configuration conf, int maxConn) {
        this.conf = conf;
        maxConn = conf.getInt("com.dataliance.hbase.maxconn", 1000);
        this.htablePool = new HTablePool(conf, maxConn);
    }
    
    public HTableInterface getHTable(final String tableName) {
        return this.htablePool.getTable(tableName.getBytes());
    }
    
    public void release(final HTableInterface htable) {
        if (htable != null) {
            try {
                htable.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    public void createTable(final String tableName, final String[] falmliys) throws IOException {
        final HBaseAdmin admin = new HBaseAdmin(this.conf);
        final HTableDescriptor tableDescripter = new HTableDescriptor(Bytes.toBytes(tableName));
        for (final String fam : falmliys) {
            final HColumnDescriptor hcolumn = new HColumnDescriptor(Bytes.toBytes(fam));
            hcolumn.setCompressionType(Compression.Algorithm.GZ);
            hcolumn.setBloomFilterType(StoreFile.BloomType.ROW);
            tableDescripter.addFamily(hcolumn);
        }
        admin.createTable(tableDescripter);
    }
    
    public void createTable(final String tableName, final String[] falmliys, final String startRow, final String stopRow, final int numRegions) throws IOException {
        this.createTable(tableName, falmliys, Bytes.toBytes(startRow), Bytes.toBytes(stopRow), numRegions, Compression.Algorithm.GZ);
    }
    
    public void createTable(final String tableName, final String[] falmliys, final String startRow, final String stopRow, final int numRegions, final Compression.Algorithm compres) throws IOException {
        this.createTable(tableName, falmliys, Bytes.toBytes(startRow), Bytes.toBytes(stopRow), numRegions, compres);
    }
    
    public void createTable(final String tableName, final String[] falmliys, final byte[] startRow, final byte[] stopRow, final int numRegions, final Compression.Algorithm compres) throws IOException {
        final HBaseAdmin admin = new HBaseAdmin(this.conf);
        final HTableDescriptor tableDescripter = new HTableDescriptor(Bytes.toBytes(tableName));
        for (final String fam : falmliys) {
            final HColumnDescriptor hcolumn = new HColumnDescriptor(Bytes.toBytes(fam));
            hcolumn.setCompressionType(compres);
            hcolumn.setBloomFilterType(StoreFile.BloomType.ROW);
            hcolumn.setMaxVersions(1);
            tableDescripter.addFamily(hcolumn);
        }
        admin.createTable(tableDescripter, startRow, stopRow, numRegions);
    }
    
    public void createTable(final String tableName, final byte[][] falmliys) throws IOException {
        final HBaseAdmin admin = new HBaseAdmin(this.conf);
        final HTableDescriptor tableDescripter = new HTableDescriptor(Bytes.toBytes(tableName));
        for (final byte[] fam : falmliys) {
            final HColumnDescriptor hcolumn = new HColumnDescriptor(fam);
            hcolumn.setCompressionType(Compression.Algorithm.GZ);
            hcolumn.setBloomFilterType(StoreFile.BloomType.ROW);
            tableDescripter.addFamily(hcolumn);
        }
        admin.createTable(tableDescripter);
    }
    
    public boolean tableExists(final byte[] tableName) throws IOException {
        final HBaseAdmin admin = new HBaseAdmin(this.conf);
        return admin.tableExists(tableName);
    }
    
    public boolean tableExists(final String tableName) throws IOException {
        final HBaseAdmin admin = new HBaseAdmin(this.conf);
        return admin.tableExists(tableName);
    }
    
    public void drop(final String tableName) throws IOException {
        final HBaseAdmin admin = new HBaseAdmin(this.conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }
    
    public void drop(final byte[] tableName) throws IOException {
        final HBaseAdmin admin = new HBaseAdmin(this.conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }
    
    public static synchronized HTableFactory getHTableFactory(final Configuration conf, final int maxConn) {
        if (HTableFactory.htableFactory == null) {
            HTableFactory.htableFactory = new HTableFactory(conf, maxConn);
        }
        return HTableFactory.htableFactory;
    }
    
    public static HTableFactory getHTableFactory(final Configuration conf) {
        return getHTableFactory(conf, 1000);
    }
    
    public Configuration getConf() {
        return this.conf;
    }
    
    public void setConf(final Configuration conf) {
        this.conf = conf;
    }
}
