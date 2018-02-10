package com.dataliance.hbase.manager;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.*;

import com.dataliance.hbase.*;
import com.dataliance.util.*;

public class DeleteRecode
{
    private HTableFactory htableFactory;
    private Configuration conf;
    
    public DeleteRecode(final Configuration conf) {
        this.conf = conf;
        this.htableFactory = HTableFactory.getHTableFactory(conf);
    }
    
    public void delete(final String tableName, final String startRow, long limit) throws Exception {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        final Scan scan = new Scan();
        if (!StringUtil.isEmpty(startRow)) {
            scan.setStartRow(Bytes.toBytes(startRow));
        }
        limit = ((limit <= 0L) ? Long.MAX_VALUE : limit);
        final ResultScanner rs = htable.getScanner(scan);
        if (rs != null) {
            for (long i = 0L; i < limit; ++i) {
                final Result result = rs.next();
                if (result != null) {
                    final Delete delete = new Delete(result.getRow());
                    htable.delete(delete);
                    System.out.println("delete lan = " + i);
                }
            }
        }
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
        final DeleteRecode dr = new DeleteRecode(conf);
        dr.delete(tableName, startRow, limit);
    }
    
    public static void printUsage() {
        System.err.println("<tableName> [-start <startRow>] [-limit <limit>]");
    }
}
