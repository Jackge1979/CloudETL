package com.dataliance.hbase.table.query;

import com.dataliance.hbase.query.*;
import com.dataliance.hbase.table.vo.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.*;
import com.dataliance.service.util.*;
import java.io.*;
import java.util.*;

public class URLQuery extends Query<URLVO>
{
    private byte[] family;
    private byte[] qualifier;
    
    public URLQuery(final Configuration conf, final String tableName) {
        super(conf, tableName);
        this.family = Bytes.toBytes(conf.get("hbase.table.family"));
        this.qualifier = Bytes.toBytes(conf.get("hbase.table.qualifier"));
    }
    
    @Override
    protected URLVO parse(final Result result) throws IOException {
        final URLVO urlVO = new URLVO();
        final String rowKey = Bytes.toString(result.getRow());
        urlVO.setRowKey(rowKey);
        final String[] vs = Constants.split(rowKey);
        urlVO.setType(Integer.parseInt(vs[0]));
        if (vs.length > 1) {
            urlVO.setUrl(vs[1]);
        }
        final byte[] value = result.getValue(this.family, this.qualifier);
        if (value != null) {
            urlVO.setStatus(Bytes.toString(value));
        }
        return urlVO;
    }
    
    public List<URLVO> scan(final String type) throws IOException {
        return this.scan(type, -1L);
    }
    
    public List<URLVO> scan(final String type, final long limit) throws IOException {
        return this.scan(type, -1L, limit);
    }
    
    public List<URLVO> scan(final String type, final long skip, final long limit) throws IOException {
        return this.scanByStart(type, null, skip, limit);
    }
    
    public List<URLVO> scanByStart(final String type, String startRow, final long skip, final long limit) throws IOException {
        if (startRow == null) {
            startRow = this.createStartRow(type);
        }
        final String endRow = this.createEndRow(type);
        return this.scan(startRow, endRow, skip, limit);
    }
    
    public List<URLVO> scanByURL(final String type, final String url) throws IOException {
        return this.scanByURL(type, url, null, -1L, -1L);
    }
    
    public List<URLVO> scanByURL(final String type, final String url, final long limi) throws IOException {
        return this.scanByURL(type, url, null, -1L, limi);
    }
    
    public List<URLVO> scanByURL(final String type, final String url, final long skip, final long limi) throws IOException {
        return this.scanByURL(type, url, null, skip, limi);
    }
    
    public List<URLVO> scanByURL(final String type, final String url, String startRow, final long skip, final long limit) throws IOException {
        if (startRow == null) {
            startRow = type + "|" + url;
        }
        final String endRow = type + "|" + url + "~";
        return this.scan(startRow, endRow, skip, limit);
    }
    
    public long getTotalByURL(final String type, final String url) throws IOException {
        final String startRow = type + "|" + url;
        final String endRow = type + "|" + url + "~";
        return this.getTotal(startRow, endRow);
    }
    
    public long getTotal(final String type) throws IOException {
        final String startRow = this.createStartRow(type);
        final String endRow = this.createEndRow(type);
        return this.getTotal(startRow, endRow);
    }
    
    private String createStartRow(final String type) {
        return type + "|";
    }
    
    private String createEndRow(final String type) {
        return type + "~";
    }
}
