package com.dataliance.hbase.table.create;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.*;
import com.cms.framework.model.bigdata.*;
import com.dataliance.util.*;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.regionserver.*;
import java.io.*;
import java.util.*;

public class HtableManager extends Configured
{
    private HBaseAdmin admin;
    
    public HtableManager(final Configuration conf) throws MasterNotRunningException, ZooKeeperConnectionException {
        super(conf);
        this.admin = new HBaseAdmin(conf);
    }
    
    public boolean createTable(final HBaseTable table) throws IOException {
        final HTableDescriptor tableDescripter = new HTableDescriptor(Bytes.toBytes(table.getTableName()));
        for (final HBaseFamilyCloumn fam : table.getFamilyCloumns()) {
            final HColumnDescriptor hcolumn = new HColumnDescriptor(Bytes.toBytes(fam.getFamilyName()));
            if (!StringUtil.isEmpty(fam.getCompression())) {
                final Compression.Algorithm compressType = Compression.Algorithm.valueOf(fam.getCompression());
                if (compressType != null) {
                    hcolumn.setCompressionType(compressType);
                }
            }
            if (!StringUtil.isEmpty(fam.getBloomFilter())) {
                final StoreFile.BloomType bloomType = StoreFile.BloomType.valueOf(fam.getBloomFilter());
                if (bloomType != null) {
                    hcolumn.setBloomFilterType(bloomType);
                }
            }
            if (!StringUtil.isEmpty(fam.isInMemory())) {
                hcolumn.setInMemory(StringUtil.toBoolean(fam.isInMemory()));
            }
            if (fam.getVersion() > 0) {
                hcolumn.setMaxVersions(fam.getVersion());
            }
            tableDescripter.addFamily(hcolumn);
        }
        this.admin.createTable(tableDescripter);
        return true;
    }
    
    public boolean deleteTable(final String tableName) throws IOException {
        if (this.admin.tableExists(tableName)) {
            this.admin.disableTable(tableName);
            this.admin.deleteTable(tableName);
            return true;
        }
        return false;
    }
    
    public HBaseTable getTableDescription(final String tableName) throws IOException {
        final HBaseTable table = new HBaseTable();
        final HTableDescriptor tableDescripter = this.admin.getTableDescriptor(Bytes.toBytes(tableName));
        table.setTableName(tableName);
        final List<HBaseFamilyCloumn> fams = new ArrayList<HBaseFamilyCloumn>();
        for (final HColumnDescriptor hcolumn : tableDescripter.getColumnFamilies()) {
            final HBaseFamilyCloumn fam = new HBaseFamilyCloumn();
            fam.setBloomFilter(hcolumn.getBloomFilterType().toString());
            fam.setCompression(hcolumn.getCompression().toString());
            fam.setFamilyName(Bytes.toString(hcolumn.getName()));
            fam.setInMemory(hcolumn.isInMemory());
            fam.setVersion(hcolumn.getMaxVersions());
            fams.add(fam);
        }
        table.setFamilyCloumns(fams);
        return table;
    }
}
