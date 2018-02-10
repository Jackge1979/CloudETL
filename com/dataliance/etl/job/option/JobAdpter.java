package com.dataliance.etl.job.option;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

import com.dataliance.etl.job.vo.*;
import com.dataliance.hadoop.util.*;
import com.dataliance.hbase.*;

import org.apache.hadoop.io.*;
import java.io.*;

public class JobAdpter extends HbaseAdpter<JobInfo>
{
    public JobAdpter(final Configuration conf, final String tableName) {
        super(conf, tableName);
    }
    
    @Override
    protected Put parse(final JobInfo jobInfo) throws IOException {
        final Put put = new Put(Bytes.toBytes(jobInfo.getProgramID()));
        put.add(JobOption.family, JobOption.qualifier, WriteableUtil.toBytes((Writable)jobInfo));
        return put;
    }
}
