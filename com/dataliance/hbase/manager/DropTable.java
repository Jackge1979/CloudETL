package com.dataliance.hbase.manager;

import org.apache.hadoop.hbase.client.*;

import com.dataliance.util.*;

import org.apache.hadoop.conf.*;
import java.io.*;

public class DropTable
{
    public static void printUsage() {
        System.err.println("Usage : DropTable <tableName>");
    }
    
    public static void main(final String[] args) throws IOException {
        if (args.length < 1) {
            printUsage();
            return;
        }
        final String tableName = args[0];
        final Configuration conf = DAConfigUtil.create();
        final HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }
}
