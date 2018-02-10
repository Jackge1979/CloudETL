package com.dataliance.hbase.reader;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.*;
import java.util.*;

import com.dataliance.hbase.*;
import com.dataliance.util.*;

public class HbaseReader
{
    private HTableFactory htableFactory;
    private Configuration conf;
    
    public HbaseReader(final Configuration conf) {
        this.conf = conf;
        this.htableFactory = HTableFactory.getHTableFactory(conf);
    }
    
    public void read(final String tableName, final String startRow, long limit) throws Exception {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        final Scan scan = new Scan();
        if (!StringUtil.isEmpty(startRow)) {
            scan.setStartRow(Bytes.toBytes(startRow));
        }
        limit = ((limit <= 0L) ? Long.MAX_VALUE : limit);
        final ResultScanner rs = htable.getScanner(scan);
        if (rs != null) {
            for (int i = 0; i < limit; ++i) {
                final Result result = rs.next();
                if (result == null) {
                    break;
                }
                System.out.println(this.outputResult(result));
            }
        }
    }
    
    public String outputResult(final Result result) {
        final StringBuffer sb = new StringBuffer();
        final byte[] rowKey = result.getRow();
        sb.append("'rowkey=").append(Bytes.toString(rowKey)).append("',");
        for (final Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : result.getMap().entrySet()) {
            final byte[] family = entry.getKey();
            final String fam = Bytes.toString(family);
            final NavigableMap<byte[], NavigableMap<Long, byte[]>> value = entry.getValue();
            for (final Map.Entry<byte[], NavigableMap<Long, byte[]>> chEntry : value.entrySet()) {
                final byte[] qualifier = chEntry.getKey();
                final NavigableMap<Long, byte[]> chValue = chEntry.getValue();
                sb.append("'column=").append(fam).append(":").append(Bytes.toString(qualifier)).append("',");
                for (final Map.Entry<Long, byte[]> chChEntry : chValue.entrySet()) {
                    final long key = chChEntry.getKey();
                    sb.append("'timestamp=").append(key).append("',");
                    final byte[] chChValue = chChEntry.getValue();
                    if (chChValue != null) {
                        sb.append("'value=").append(Bytes.toStringBinary(chChValue)).append("',");
                    }
                    else {
                        sb.append("'value=',");
                    }
                }
            }
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
    
    public static void main(final String[] args) throws Exception {
        if (args.length < 1 || args.length > 5) {
            printUsage();
            return;
        }
        final String tableName = args[0];
        long limit = 0L;
        String startRow = "";
        try {
            for (int i = 1; i < args.length; i += 2) {
                if (args[i].equals("-start")) {
                    startRow = args[i + 1];
                }
                else {
                    if (!args[i].equals("-limit")) {
                        printUsage();
                        return;
                    }
                    limit = StringUtil.toLong(args[i + 1]);
                }
            }
        }
        catch (Exception e) {
            printUsage();
            return;
        }
        final Configuration conf = DAConfigUtil.create();
        final HbaseReader hr = new HbaseReader(conf);
        hr.read(tableName, startRow, limit);
    }
    
    public static void printUsage() {
        System.err.println("<tableName> [-start <startRow>] [-limit <limit>]");
    }
}
