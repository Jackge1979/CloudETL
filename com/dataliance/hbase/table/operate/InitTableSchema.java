package com.dataliance.hbase.table.operate;

import java.math.*;
import org.apache.hadoop.hbase.client.*;
import com.dataliance.service.util.*;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.util.*;
import java.io.*;
import org.apache.hadoop.hbase.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.*;
import org.slf4j.*;

public class InitTableSchema
{
    private static final Logger LOG;
    private static final boolean IS_EXISTED_DROP;
    
    public static byte[][] getHexSplits(final String startKeyNum, final String endKeyNum, final int numRegions) {
        final byte[][] splits = new byte[numRegions - 1][];
        BigInteger lowestKey = new BigInteger(startKeyNum, 10);
        final BigInteger highestKey = new BigInteger(endKeyNum, 10);
        final BigInteger range = highestKey.subtract(lowestKey);
        final BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
        lowestKey = lowestKey.add(regionIncrement);
        for (int i = 0; i < numRegions - 1; ++i) {
            final BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
            final byte[] b = String.format("%08d", key).getBytes();
            splits[i] = b;
        }
        return splits;
    }
    
    private static void createTable(final HBaseAdmin admin, final String tableName) throws IOException {
        final boolean isExisted = admin.tableExists(tableName);
        if (isExisted && !InitTableSchema.IS_EXISTED_DROP) {
            InitTableSchema.LOG.info(tableName + " table is existed!");
            return;
        }
        if (isExisted && InitTableSchema.IS_EXISTED_DROP) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        final HTableDescriptor desc = new HTableDescriptor(tableName);
        final HColumnDescriptor cfRaw = new HColumnDescriptor(ConfigUtils.getConfig().get("hbase.table.family"));
        cfRaw.setBloomFilterType(StoreFile.BloomType.ROW);
        cfRaw.setBlockCacheEnabled(true);
        cfRaw.setMaxVersions(1);
        desc.addFamily(cfRaw);
        InitTableSchema.LOG.info("creating table " + tableName);
        final String startKeyNum = ConfigUtils.getConfig().get("init.region.startkey");
        final String endKeyNum = ConfigUtils.getConfig().get("init.region.endkey");
        final int numRegions = ConfigUtils.getConfig().getInt("hbase.table.numregions", 99);
        byte[][] splits = null;
        try {
            splits = getHexSplits(startKeyNum, endKeyNum, numRegions);
        }
        catch (Exception e) {
            splits = Bytes.split(Bytes.toBytes(startKeyNum), Bytes.toBytes(endKeyNum), numRegions);
        }
        admin.createTable(desc, splits);
        InitTableSchema.LOG.info("table " + tableName + " has already created");
    }
    
    private static void createTableInMem(final HBaseAdmin admin, final String tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        final HTableDescriptor desc = new HTableDescriptor(tableName);
        final HColumnDescriptor cfRaw = new HColumnDescriptor(ConfigUtils.getConfig().get("hbase.table.family"));
        cfRaw.setBloomFilterType(StoreFile.BloomType.ROW);
        cfRaw.setMaxVersions(1);
        cfRaw.setInMemory(true);
        desc.addFamily(cfRaw);
        InitTableSchema.LOG.info("creating table " + tableName);
        admin.createTable(desc);
        InitTableSchema.LOG.info("table " + tableName + " has already created");
    }
    
    private static final Options buildOptions() {
        final Options options = new Options();
        options.addOption("table", true, "table name in hbase");
        options.addOption("inmem", false, "InMem true or false");
        return options;
    }
    
    private static final void printUsage(final Options options) {
        final HelpFormatter help = new HelpFormatter();
        help.printHelp(InitTableSchema.class.getName(), options);
    }
    
    public static void main(final String[] args) {
        final Options options = buildOptions();
        CommandLine commands = null;
        try {
            final BasicParser parser = new BasicParser();
            commands = parser.parse(options, args);
        }
        catch (ParseException e3) {
            printUsage(options);
            return;
        }
        if (!commands.hasOption("table")) {
            printUsage(options);
            return;
        }
        boolean inmem = false;
        if (commands.hasOption("inmem")) {
            try {
                inmem = Boolean.parseBoolean(commands.getOptionValue("inmem"));
            }
            catch (Exception e) {
                e.printStackTrace();
                printUsage(options);
                return;
            }
        }
        final String tableName = commands.getOptionValue("table").trim();
        final Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(conf);
            System.out.println("tableName:" + tableName);
            if (inmem) {
                createTableInMem(admin, tableName);
            }
            else {
                createTable(admin, tableName);
            }
        }
        catch (IOException e2) {
            InitTableSchema.LOG.error(e2.getMessage(), (Throwable)e2);
        }
        finally {
            if (null != admin) {
                admin = null;
            }
        }
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)InitTableSchema.class);
        IS_EXISTED_DROP = ConfigUtils.getConfig().getBoolean("table.is.existed.drop", false);
    }
}
