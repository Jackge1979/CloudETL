package com.dataliance.hbase.table.operate;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.*;
import java.io.*;
import org.apache.hadoop.hbase.client.*;
import java.util.*;
import org.apache.hadoop.hbase.*;
import org.apache.commons.logging.*;

public class TableOperator
{
    private static final Log LOG;
    
    public static void addRecord(final Configuration conf, final String tableName, final String rowKey, final String family, final String qualifier, final String value) throws IOException {
        final HTable table = new HTable(conf, tableName);
        final Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(put);
        table.close();
        TableOperator.LOG.info((Object)(" recored " + rowKey + " to table " + tableName + " ok."));
    }
    
    public static Object[] delRecord(final Configuration conf, final String tableName, final String rowKey) throws IOException, InterruptedException {
        final HTable table = new HTable(conf, tableName);
        final List<Row> list = new ArrayList<Row>();
        final Delete del = new Delete(rowKey.getBytes());
        list.add((Row)del);
        final Object[] result = table.batch((List)list);
        table.close();
        TableOperator.LOG.info((Object)("del recored " + rowKey + " ok."));
        return result;
    }
    
    public static Result getOneRecord(final Configuration conf, final String tableName, final String rowKey) throws IOException {
        final HTable table = new HTable(conf, tableName);
        final Get get = new Get(rowKey.getBytes());
        final Result result = table.get(get);
        table.close();
        return result;
    }
    
    public static ResultScanner getAllRecord(final Configuration conf, final String tableName) throws IOException {
        final HTable table = new HTable(conf, tableName);
        final Scan s = new Scan();
        final ResultScanner resultScanner = table.getScanner(s);
        table.close();
        return resultScanner;
    }
    
    public static void main(final String[] args) throws IOException {
        final Configuration conf = HBaseConfiguration.create();
        conf.addResource("bigdata-site.xml");
        final String tableName = conf.get("mobilephone.table.name", "phone");
        System.err.println(conf.get("hbase.zookeeper.quorum"));
        if ("localhost".equals(conf.get("hbase.zookeeper.quorum"))) {
            System.err.println("please load hbase-site.xml");
            System.exit(-1);
        }
        final ResultScanner resultScanner = getAllRecord(conf, tableName);
        for (final Result result : resultScanner) {
            final KeyValue[] arr$;
            final KeyValue[] keyValues = arr$ = result.raw();
            for (final KeyValue keyValue : arr$) {
                System.err.print(Bytes.toString(keyValue.getRow()) + " ");
                System.err.print(Bytes.toString(keyValue.getFamily()) + ":" + Bytes.toString(keyValue.getQualifier()) + " ");
                System.err.println(Bytes.toString(keyValue.getValue()));
            }
        }
        resultScanner.close();
    }
    
    static {
        LOG = LogFactory.getLog((Class)TableOperator.class);
    }
}
