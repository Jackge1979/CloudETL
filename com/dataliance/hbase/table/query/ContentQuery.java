package com.dataliance.hbase.table.query;

import com.dataliance.hbase.query.*;
import com.dataliance.hbase.table.verify.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.*;
import com.dataliance.service.util.*;
import java.io.*;
import com.dataliance.hbase.table.vo.*;
import java.util.*;

public class ContentQuery extends Query<ContentVO>
{
    private static final String MAX_CHAR = "~";
    private byte[] family;
    private ContentVerify conVerify;
    private CategoryQuery categoryQuery;
    private CategoryVerify categoryVerify;
    
    public ContentQuery(final Configuration conf, final String contentName, final String categoryName) {
        super(conf, contentName);
        this.family = Bytes.toBytes(conf.get("hbase.table.classifer.statistic.family.name"));
        this.conVerify = new ContentVerify(conf, contentName);
        this.categoryQuery = new CategoryQuery(conf, categoryName);
        this.categoryVerify = new CategoryVerify(conf, categoryName);
    }
    
    @Override
    protected ContentVO parse(final Result result) throws IOException {
        final ContentVO con = new ContentVO();
        final String url = Bytes.toString(result.getRow());
        con.setRowKey(url);
        con.setUrl(url);
        con.setTitle(this.getValue(result.getValue(this.family, Constants.QUALIFIER_TITLE)));
        con.setContent(this.getValue(result.getValue(this.family, Constants.QUALIFIER_CONTENT)));
        con.setCategoryText(this.getValue(result.getValue(this.family, Constants.QUALIFIER_CATEGORY_TEXT)));
        con.setCategoryUrl(this.getValue(result.getValue(this.family, Constants.QUALIFIER_CATEGORY_URL)));
        con.setCategoryVerify(this.getValue(result.getValue(this.family, Constants.QUALIFIER_CATEGORY_VERIFY)));
        return con;
    }
    
    public List<ContentVO> scan(final String category) throws IOException {
        return this.scan(category, -1L);
    }
    
    public List<ContentVO> scan(final String category, final long limit) throws IOException {
        return this.scan(category, -1L, limit);
    }
    
    public List<ContentVO> scan(final String category, final long skip, final long limit) throws IOException {
        return this.scanByStart(category, null, skip, limit);
    }
    
    public List<ContentVO> scanByStart(final String category, final String startRow, final long skip, final long limit) throws IOException {
        final List<Category> categorys = this.categoryQuery.scanByStart(category, startRow, skip, limit);
        final List<ContentVO> results = new ArrayList<ContentVO>();
        if (categorys != null) {
            for (final Category cate : categorys) {
                final ContentVO content = this.doGet(cate.getUrl());
                if (content != null) {
                    content.setCategory(cate);
                    results.add(content);
                }
            }
        }
        return results.isEmpty() ? null : results;
    }
    
    private String getValue(final byte[] b) {
        if (b != null) {
            return Bytes.toString(b);
        }
        return null;
    }
    
    public long getTotal(final String category) throws IOException {
        return this.categoryQuery.getTotal(category);
    }
    
    public void verify(final ContentVO conVO, final String categoryVerify) throws IOException {
        this.categoryVerify.verify(conVO, categoryVerify);
        conVO.setCategoryVerify(categoryVerify);
        this.conVerify.insert(conVO);
    }
    
    public void verify(final String url, final String categoryVerify) throws IOException {
        final ContentVO conVO = this.doGet(url);
        if (conVO != null) {
            this.verify(conVO, categoryVerify);
            return;
        }
        throw new IOException("url = " + url + " is not exist!");
    }
    
    public List<ContentVO> scanByURL(final String url) throws IOException {
        return this.scanByURL(url, null);
    }
    
    public List<ContentVO> scanByURL(final String url, final String startRow) throws IOException {
        return this.scanByURL(url, startRow, -1L, -1L);
    }
    
    public List<ContentVO> scanByURL(final String url, final long skip, final long limit) throws IOException {
        return this.scanByURL(url, null, skip, limit);
    }
    
    public List<ContentVO> scanByURL(String url, String startRow, final long skip, final long limit) throws IOException {
        url = this.parseURL(url);
        if (startRow == null) {
            startRow = url;
        }
        final String endRow = this.createEndRow(url);
        return this.scan(startRow, endRow, skip, limit);
    }
    
    public long getTotalByURL(String url) throws IOException {
        url = this.parseURL(url);
        final String endRow = this.createEndRow(url);
        return this.getTotal(url, endRow);
    }
    
    private String createEndRow(final String url) {
        return url + "~";
    }
    
    private String parseURL(final String url) {
        return url.startsWith("http://") ? url : ("http://" + url);
    }
}
