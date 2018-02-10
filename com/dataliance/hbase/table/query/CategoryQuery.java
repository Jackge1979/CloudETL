package com.dataliance.hbase.table.query;

import com.dataliance.hbase.query.*;
import com.dataliance.hbase.table.vo.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import java.io.*;
import com.dataliance.service.util.*;
import java.util.*;

public class CategoryQuery extends Query<Category>
{
    public CategoryQuery(final Configuration conf, final String tableName) {
        super(conf, tableName);
    }
    
    @Override
    protected Category parse(final Result result) throws IOException {
        final Category category = new Category();
        final String rowKey = Bytes.toString(result.getRow());
        category.setRowKey(rowKey);
        final String[] vs = Constants.split(rowKey, 2);
        category.setCategory(vs[0].replaceFirst("^0+", ""));
        if (vs.length == 2) {
            category.setUrl(vs[1]);
        }
        return category;
    }
    
    public Category doGet(final String category, final String url) throws IOException {
        return this.doGet(CategoryUtil.createRowKey(category, url));
    }
    
    public List<Category> scan(final String category) throws IOException {
        return this.scan(category, -1L);
    }
    
    public List<Category> scan(final String category, final long limit) throws IOException {
        return this.scan(category, -1L, limit);
    }
    
    public List<Category> scan(final String category, final long skip, final long limit) throws IOException {
        final String startRow = this.createStartRow(category);
        final String endRow = this.createEndRow(category);
        return this.scan(startRow, endRow, skip, limit);
    }
    
    public List<Category> scanByStart(final String category, String startRow, final long skip, final long limit) throws IOException {
        if (startRow == null) {
            startRow = this.createStartRow(category);
        }
        final String endRow = this.createEndRow(category);
        return this.scan(startRow, endRow, skip, limit);
    }
    
    public long getTotal(final String category) throws IOException {
        final String startRow = this.createStartRow(category);
        final String endRow = this.createEndRow(category);
        return this.getTotal(startRow, endRow);
    }
    
    private String createStartRow(final String category) {
        return CategoryUtil.fillCategory(category) + "|";
    }
    
    private String createEndRow(final String category) {
        return CategoryUtil.fillCategory(category) + "~";
    }
}
