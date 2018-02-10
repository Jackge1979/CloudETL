package com.dataliance.analysis.data.assess;

import com.dataliance.analysis.data.assess.vo.*;
import com.dataliance.hbase.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import com.dataliance.service.util.*;
import com.dataliance.hbase.table.vo.*;
import java.io.*;

public class AssessAdpter extends HbaseAdpter<AssessRecod>
{
    private static final byte[] family;
    
    public AssessAdpter(final Configuration conf, final String tableName) {
        super(conf, tableName);
    }
    
    @Override
    protected Put parse(final AssessRecod a) throws IOException {
        final Put put = new Put(Bytes.toBytes(a.getRowKey()));
        final ContentVO content = a.getContent();
        put.add(AssessAdpter.family, Constants.QUALIFIER_TITLE, this.toBytes(content.getTitle()));
        put.add(AssessAdpter.family, Constants.QUALIFIER_CONTENT, this.toBytes(content.getContent()));
        put.add(AssessAdpter.family, Constants.QUALIFIER_CATEGORY_TEXT, this.toBytes(content.getCategoryText()));
        put.add(AssessAdpter.family, Constants.QUALIFIER_CATEGORY_URL, this.toBytes(content.getCategoryUrl()));
        put.add(AssessAdpter.family, Constants.QUALIFIER_CATEGORY_VERIFY, this.toBytes(content.getCategoryVerify()));
        put.add(AssessAdpter.family, AssessManager.VERIFY, this.toBytes(a.getCategoryVerify()));
        return put;
    }
    
    private byte[] toBytes(final String value) {
        return (byte[])((value == null) ? null : Bytes.toBytes(value));
    }
    
    static {
        family = Bytes.toBytes("I");
    }
}
