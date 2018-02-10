package com.dataliance.hbase.table.operate;

import org.apache.hadoop.conf.*;
import java.io.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.*;
import org.apache.commons.logging.*;

public class TableDDL
{
    private static final Log LOG;
    
    public static boolean createPhoneTable(final Configuration conf, final String tableName, final String... columnFamily) throws IOException {
        return createPhoneTable(conf, true, tableName, columnFamily);
    }
    
    public static boolean createPhoneTable(final Configuration conf, final String tableName, final byte[]... columnFamily) throws IOException {
        return createPhoneTable(conf, true, tableName, columnFamily);
    }
    
    public static boolean createPhoneTable(final Configuration conf, final boolean checkIsExist, final String tableName, final byte[]... columnFamily) throws IOException {
        final HBaseAdmin admin = new HBaseAdmin(conf);
        if (checkIsExist && admin.tableExists(tableName)) {
            TableDDL.LOG.info((Object)String.format("%s already exists!", tableName));
        }
        else {
            final HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (final byte[] family : columnFamily) {
                final HColumnDescriptor hcolumnDescriptor = new HColumnDescriptor(family);
                hcolumnDescriptor.setBloomFilterType(StoreFile.BloomType.ROW);
                hcolumnDescriptor.setBlockCacheEnabled(true);
                hcolumnDescriptor.setMaxVersions(1);
                tableDesc.addFamily(hcolumnDescriptor);
            }
            admin.createTable(tableDesc);
            TableDDL.LOG.info((Object)String.format("create table %s ok.", tableName));
        }
        return true;
    }
    
    public static boolean createPhoneTable(final Configuration conf, final boolean checkIsExist, final String tableName, final String... columnFamily) throws IOException {
        final HBaseAdmin admin = new HBaseAdmin(conf);
        if (checkIsExist && admin.tableExists(tableName)) {
            TableDDL.LOG.info((Object)String.format("%s already exists!", tableName));
        }
        else {
            final HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (final String family : columnFamily) {
                final HColumnDescriptor hcolumnDescriptor = new HColumnDescriptor(family);
                hcolumnDescriptor.setBloomFilterType(StoreFile.BloomType.ROW);
                hcolumnDescriptor.setBlockCacheEnabled(true);
                hcolumnDescriptor.setMaxVersions(1);
                tableDesc.addFamily(hcolumnDescriptor);
            }
            admin.createTable(tableDesc);
            TableDDL.LOG.info((Object)String.format("create table %s ok.", tableName));
        }
        return true;
    }
    
    public static void deletePhoneTable(final Configuration conf, final String tableName) throws Exception {
        try {
            final HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            TableDDL.LOG.info((Object)String.format("delete table %s ok.", tableName));
        }
        catch (MasterNotRunningException e) {
            e.printStackTrace();
        }
        catch (ZooKeeperConnectionException e2) {
            e2.printStackTrace();
        }
    }
    
    public static boolean isExistedTable(final Configuration conf, final String tableName) {
        boolean isExist = false;
        try {
            final HBaseAdmin admin = new HBaseAdmin(conf);
            isExist = admin.tableExists(tableName);
        }
        catch (MasterNotRunningException e) {
            e.printStackTrace();
        }
        catch (ZooKeeperConnectionException e2) {
            e2.printStackTrace();
        }
        catch (IOException e3) {
            e3.printStackTrace();
        }
        return isExist;
    }
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = HBaseConfiguration.create();
        conf.addResource("bigdata-site.xml");
        final String tableName = conf.get("mobilephone.table.name", "phone");
        createPhoneTable(conf, tableName, "info");
    }
    
    static {
        LOG = LogFactory.getLog((Class)TableDDL.class);
    }
}
