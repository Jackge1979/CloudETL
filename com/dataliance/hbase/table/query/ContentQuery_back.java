package com.dataliance.hbase.table.query;

import com.dataliance.hbase.table.vo.*;
import com.dataliance.hbase.table.verify.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.*;
import com.dataliance.service.util.*;
import java.io.*;

import com.dataliance.hbase.query.*;
import com.dataliance.util.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import java.util.*;

public class ContentQuery_back extends Query<ContentVO>
{
    private static final String MAX_CHAR = "~";
    private static final int MAX_CATEGORY_LEN = 5;
    private static final String CATEGORY_ALL = "-1";
    private byte[] family;
    private ContentVerify conVerify;
    
    public ContentQuery_back(final Configuration conf, final String tableName) {
        super(conf, tableName);
        this.family = Bytes.toBytes(conf.get("hbase.table.classifer.statistic.family.name"));
        this.conVerify = new ContentVerify(conf, tableName);
    }
    
    @Override
    protected ContentVO parse(final Result result) throws IOException {
        final ContentVO con = new ContentVO();
        final String rowKey = Bytes.toString(result.getRow());
        con.setRowKey(rowKey);
        final String[] vs = Constants.split(rowKey);
        con.setUrl(vs[1]);
        con.setTitle(this.getValue(result.getValue(this.family, Constants.QUALIFIER_TITLE)));
        con.setContent(this.getValue(result.getValue(this.family, Constants.QUALIFIER_CONTENT)));
        con.setCategoryText(this.getValue(result.getValue(this.family, Constants.QUALIFIER_CATEGORY_TEXT)));
        con.setCategoryUrl(this.getValue(result.getValue(this.family, Constants.QUALIFIER_CATEGORY_URL)));
        con.setCategoryVerify(this.getValue(result.getValue(this.family, Constants.QUALIFIER_CATEGORY_VERIFY)));
        return con;
    }
    
    private String createPrefixRowKey(final String category) {
        return NumFormat.prefixFill(category, 5, "0");
    }
    
    public List<ContentVO> scan(final String category) throws IOException {
        return this.scan(category, -1L);
    }
    
    public List<ContentVO> scan(final String category, final long limit) throws IOException {
        return this.scan(category, -1L, limit);
    }
    
    public List<ContentVO> scan(final String category, final long skip, final long limit) throws IOException {
        final String startRow = this.createStartRow(category);
        final String endRow = this.createEndRow(category);
        return this.scan(startRow, endRow, skip, limit);
    }
    
    public List<ContentVO> scanByStart(final String category, String startRow, final long skip, final long limit) throws IOException {
        if (startRow == null) {
            startRow = this.createStartRow(category);
        }
        final String endRow = this.createEndRow(category);
        return this.scan(startRow, endRow, skip, limit);
    }
    
    private String createStartRow(final String category) {
        return this.createPrefixRowKey(category) + "@#\\$";
    }
    
    private String createEndRow(final String category) {
        return this.createPrefixRowKey(category) + "~";
    }
    
    public ContentVO doGet(final String category, final String url) throws IOException {
        final String prefix = this.createPrefixRowKey(category);
        final String rowKey = prefix + "|" + this.parseURL(url);
        return this.doGet(rowKey);
    }
    
    private String getValue(final byte[] b) {
        if (b != null) {
            return Bytes.toString(b);
        }
        return null;
    }
    
    public long getTotal(final String category) throws IOException {
        final Scan scan = new Scan();
        final String prefix = this.createPrefixRowKey(category);
        final PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes(prefix));
        scan.setFilter((Filter)prefixFilter);
        return this.getTotal(scan);
    }
    
    public void verify(final ContentVO src, final String categoryVerify) throws IOException {
        this.conVerify.verify(src, categoryVerify);
    }
    
    public void verify(final String rowKey, final String categoryVerify) throws IOException {
        final ContentVO conVO = this.doGet(rowKey);
        if (conVO != null) {
            this.conVerify.verify(conVO, categoryVerify);
            return;
        }
        throw new IOException("rowKey = " + rowKey + " is not exist!");
    }
    
    public List<ContentVO> scan(final List<String> categorys, final String selCategory, final String url, final boolean accurate) throws IOException {
        return this.scan(categorys, selCategory, url, accurate, -1L, -1L);
    }
    
    public List<ContentVO> scan(final List<String> categorys, final String selCategory, final String url, final boolean accurate, final long skip, final long limit) throws IOException {
        if (accurate) {
            final List<ContentVO> results = new ArrayList<ContentVO>();
            if ("-1".equals(selCategory)) {
                for (final String category : categorys) {
                    final ContentVO con = this.doGet(category, url);
                    if (con != null) {
                        results.add(con);
                    }
                }
            }
            else {
                final ContentVO con2 = this.doGet(selCategory, url);
                if (con2 != null) {
                    results.add(con2);
                }
            }
            return results.isEmpty() ? null : results;
        }
        if ("-1".equals(selCategory)) {
            final FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            final Filter pageFilter = this.getPageFilter(skip, limit);
            if (pageFilter != null) {
                filter.addFilter(pageFilter);
            }
            final FilterList prefixFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            for (final String category2 : categorys) {
                final String prefix = this.createPrefixRowKey(category2) + "|" + this.parseURL(url);
                final Filter f = (Filter)new PrefixFilter(Bytes.toBytes(prefix));
                prefixFilters.addFilter(f);
            }
            filter.addFilter((Filter)prefixFilters);
            final Scan scan = new Scan();
            if (pageFilter != null) {
                scan.setFilter((Filter)filter);
            }
            else {
                scan.setFilter((Filter)prefixFilters);
            }
            return this.scan(scan, skip);
        }
        final String startRow = this.createPrefixRowKey(selCategory) + "|" + this.parseURL(url);
        final String endRow = this.createPrefixRowKey(selCategory) + "|" + this.parseURL(url) + "~";
        return this.scan(startRow, endRow, skip, limit);
    }
    
    public long getTotal(final List<String> categorys, final String selCategory, final String url, final boolean accurate) throws IOException {
        if (accurate) {
            long total = 0L;
            if ("-1".equals(selCategory)) {
                for (final String category : categorys) {
                    final ContentVO con = this.doGet(category, url);
                    if (con != null) {
                        ++total;
                    }
                }
            }
            else {
                final ContentVO con2 = this.doGet(selCategory, url);
                if (con2 != null) {
                    ++total;
                }
            }
            return total;
        }
        if ("-1".equals(selCategory)) {
            final FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            for (final String category2 : categorys) {
                final String prefix = this.createPrefixRowKey(category2) + "|" + this.parseURL(url);
                final Filter f = (Filter)new PrefixFilter(Bytes.toBytes(prefix));
                filter.addFilter(f);
            }
            final Scan scan = new Scan();
            scan.setFilter((Filter)filter);
            return this.getTotal(scan);
        }
        final String startRow = this.createPrefixRowKey(selCategory) + "|" + this.parseURL(url);
        final String endRow = this.createPrefixRowKey(selCategory) + "|" + this.parseURL(url) + "~";
        return this.getTotal(startRow, endRow);
    }
    
    private String parseURL(final String url) {
        return url.startsWith("http://") ? url : ("http://" + url);
    }
}
