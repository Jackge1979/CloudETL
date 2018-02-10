package com.dataliance.bigdata.hbase.mapper;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import com.dataliance.bdp.hbase.*;
import org.apache.hadoop.hbase.util.*;
import java.io.*;
import com.dataliance.service.util.*;
import org.apache.commons.lang.*;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.*;

public class BasedUrlImportMapper extends Mapper<LongWritable, Text, Text, Text>
{
    private static final Logger LOG;
    private HBaseHelper hBaseHelper;
    private HTableInterface classifyStatisticTable;
    private HTableInterface classifyTable;
    private static byte[] CLASSIFY_FAMILY;
    private static byte[] CLASSIFY_STATISTIC_FAMILY;
    private static byte[] CLASSIFY_STATISTIC_QUALIFIER;
    
    protected void setup(final Mapper.Context context) throws IOException, InterruptedException {
        this.hBaseHelper = HBaseHelper.getInstance();
        try {
            BasedUrlImportMapper.CLASSIFY_FAMILY = Bytes.toBytes(context.getConfiguration().get("hbase.table.classifier.family.name"));
            (this.classifyTable = this.hBaseHelper.getTable(context.getConfiguration().get("hbase.table.classifier.name"))).setAutoFlush(false);
            this.classifyTable.setWriteBufferSize(2097152L);
            BasedUrlImportMapper.CLASSIFY_STATISTIC_FAMILY = Bytes.toBytes(context.getConfiguration().get("hbase.table.classifer.statistic.family.name"));
            BasedUrlImportMapper.CLASSIFY_STATISTIC_QUALIFIER = Bytes.toBytes(context.getConfiguration().get("hbase.table.classifer.statistic.qualifier.name"));
            (this.classifyStatisticTable = this.hBaseHelper.getTable(context.getConfiguration().get("hbase.table.classifer.statistic.name"))).setAutoFlush(false);
            this.classifyStatisticTable.setWriteBufferSize(2097152L);
        }
        catch (IOException e) {
            BasedUrlImportMapper.LOG.error("Cannot initial the htable", (Throwable)e);
        }
    }
    
    protected void map(final LongWritable key, final Text value, final Mapper.Context context) throws IOException {
        if (value == null || "".equals(value.toString().trim())) {
            return;
        }
        final String[] fieldValues = value.toString().split("\t");
        if (fieldValues.length != 2) {
            return;
        }
        final String url = fieldValues[0];
        final String categoryId = fieldValues[1];
        Put put = new Put(Bytes.toBytes(url));
        put.setWriteToWAL(true);
        put.add(BasedUrlImportMapper.CLASSIFY_FAMILY, Constants.QUALIFIER_CATEGORY_URL, Bytes.toBytes(categoryId));
        this.classifyTable.put(put);
        final byte[] rowkey = Bytes.toBytes(StringUtils.leftPad(categoryId, 5, '0') + "|" + url);
        put = new Put(rowkey);
        put.setWriteToWAL(true);
        put.add(BasedUrlImportMapper.CLASSIFY_STATISTIC_FAMILY, BasedUrlImportMapper.CLASSIFY_STATISTIC_QUALIFIER, (byte[])null);
        this.classifyStatisticTable.put(put);
    }
    
    protected void cleanup(final Mapper.Context context1) throws IOException, InterruptedException {
        this.hBaseHelper.close(this.classifyTable);
        this.hBaseHelper.close(this.classifyStatisticTable);
    }
    
    private void countByCategoryType(final HTable hTable, final String categoryId, final long urlCount) {
        try {
            final byte[] rowkey = Bytes.toBytes(StringUtils.leftPad(categoryId, 5, '0'));
            hTable.incrementColumnValue(rowkey, BasedUrlImportMapper.CLASSIFY_STATISTIC_FAMILY, BasedUrlImportMapper.CLASSIFY_STATISTIC_QUALIFIER, urlCount);
        }
        catch (IOException e) {
            BasedUrlImportMapper.LOG.error("Increment total traffic error: " + categoryId, (Throwable)e);
        }
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)BasedUrlImportMapper.class);
    }
}
