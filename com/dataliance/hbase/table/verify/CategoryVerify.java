package com.dataliance.hbase.table.verify;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.*;
import com.dataliance.service.util.*;
import java.io.*;
import com.dataliance.hbase.table.vo.*;
import com.dataliance.hbase.*;
import com.dataliance.util.*;

public class CategoryVerify extends HbaseAdpter<Category>
{
    private static final byte[] NULL_BYTE;
    private byte[] family;
    private byte[] qualifier;
    
    public CategoryVerify(final Configuration conf, final String tableName) {
        super(conf, tableName);
        this.family = Bytes.toBytes(conf.get("hbase.table.family"));
        this.qualifier = Bytes.toBytes(conf.get("hbase.table.qualifier"));
    }
    
    @Override
    protected Put parse(final Category cate) throws IOException {
        final Put put = new Put(Bytes.toBytes(CategoryUtil.createRowKey(cate.getCategory(), cate.getUrl())));
        put.add(this.family, this.qualifier, CategoryVerify.NULL_BYTE);
        return put;
    }
    
    public void verify(final ContentVO src, final String categoryVerify) throws IOException {
        String category = src.getCategoryVerify();
        if (StringUtil.isEmpty(category)) {
            category = src.getCategoryUrl();
        }
        if (StringUtil.isEmpty(category)) {
            category = src.getCategoryText();
        }
        this.verify(src.getUrl(), category, categoryVerify);
    }
    
    public void verify(final String url, final String srcCategory, final String categoryVerify) throws IOException {
        final String rowKey = CategoryUtil.createRowKey(srcCategory, url);
        this.delete(rowKey);
        final Category cate = new Category();
        cate.setCategory(categoryVerify);
        cate.setUrl(url);
        this.insert(cate);
    }
    
    static {
        NULL_BYTE = new byte[0];
    }
}
