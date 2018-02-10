package com.dataliance.hbase.table.verify;

import com.dataliance.hbase.table.vo.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.*;
import com.dataliance.service.util.*;
import java.io.*;

import com.dataliance.hbase.*;
import com.dataliance.util.*;

public class ContentVerify extends HbaseAdpter<ContentVO>
{
    private byte[] family;
    
    public ContentVerify(final Configuration conf, final String tableName) {
        super(conf, tableName);
        this.family = Bytes.toBytes(conf.get("hbase.table.classifer.statistic.family.name"));
    }
    
    @Override
    protected Put parse(final ContentVO content) throws IOException {
        final Put put = new Put(Bytes.toBytes(content.getRowKey()));
        put.add(this.family, Constants.QUALIFIER_TITLE, this.toBytes(content.getTitle()));
        put.add(this.family, Constants.QUALIFIER_CATEGORY_TEXT, this.toBytes(content.getCategoryText()));
        put.add(this.family, Constants.QUALIFIER_CATEGORY_URL, this.toBytes(content.getCategoryUrl()));
        put.add(this.family, Constants.QUALIFIER_CATEGORY_VERIFY, this.toBytes(content.getCategoryVerify()));
        put.add(this.family, Constants.QUALIFIER_CONTENT, this.toBytes(content.getContent()));
        return put;
    }
    
    public void verify(final ContentVO src, final String categoryVerify) throws IOException {
        this.delete(src.getRowKey());
        final String rowKey = NumFormat.prefixFill(categoryVerify, 5, "0") + "|" + src.getUrl();
        src.setRowKey(rowKey);
        src.setCategoryVerify(categoryVerify);
        this.insert(src);
    }
    
    private byte[] toBytes(final String value) {
        if (null != value) {
            return Bytes.toBytes(value);
        }
        return new byte[0];
    }
}
