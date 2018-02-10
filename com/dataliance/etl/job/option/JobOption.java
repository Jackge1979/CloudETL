package com.dataliance.etl.job.option;

import org.apache.hadoop.conf.*;
import java.io.*;
import org.apache.hadoop.hbase.client.*;

import com.dataliance.etl.job.vo.*;
import com.dataliance.hadoop.util.*;
import com.dataliance.hbase.*;
import com.dataliance.hbase.query.*;

import org.apache.hadoop.io.*;
import com.dataliance.service.util.*;

import org.slf4j.*;
import org.apache.hadoop.hbase.util.*;

public class JobOption extends Query<JobInfo>
{
    private static final Logger LOG;
    public static final String CONF_TABLE_JOB_STATUS = "com.DA.job.hbase.table";
    public static final String TABLE_JOB_STATUS = "__JOB_STATUS__";
    public static final byte[] family;
    public static final byte[] qualifier;
    private JobAdpter jobAdpter;
    
    public JobOption(final Configuration conf) {
        this(conf, conf.get("com.DA.job.hbase.table", "__JOB_STATUS__"));
    }
    
    public JobOption(final Configuration conf, final String tableName) {
        super(conf, tableName);
        this.jobAdpter = new JobAdpter(conf, tableName);
    }
    
    public JobInfo get(final String programID) throws IOException {
        return this.doGet(programID);
    }
    
    public void insert(final JobInfo jobInfo) throws IOException {
        this.jobAdpter.insert(jobInfo);
    }
    
    @Override
    protected JobInfo parse(final Result result) throws IOException {
        final JobInfo jobInfo = new JobInfo();
        final byte[] data = result.getValue(JobOption.family, JobOption.qualifier);
        if (data != null) {
            WriteableUtil.read((Writable)jobInfo, data);
        }
        return jobInfo;
    }
    
    public static void main(final String[] args) throws IOException {
        final String usAge = "Usage : -create [tableName]";
        if (args.length == 0) {
            System.err.println(usAge);
            return;
        }
        final Configuration conf = ConfigUtils.getConfig();
        String tableName = conf.get("com.DA.job.hbase.table", "__JOB_STATUS__");
        if (!args[0].equals("-create")) {
            System.err.println(usAge);
            return;
        }
        if (args.length == 2) {
            tableName = args[1];
        }
        JobOption.LOG.info("Will create tableName = " + tableName);
        final HTableFactory factory = HTableFactory.getHTableFactory(conf);
        if (!factory.tableExists(tableName)) {
            factory.createTable(tableName, new String[] { "I" });
        }
        else {
            JobOption.LOG.info("TableName = " + tableName + "is exists!");
        }
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)JobOption.class);
        family = Bytes.toBytes("I");
        qualifier = Bytes.toBytes("Q");
    }
}
