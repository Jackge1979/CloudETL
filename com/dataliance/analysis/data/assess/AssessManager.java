package com.dataliance.analysis.data.assess;

import com.dataliance.hbase.table.query.*;
import org.apache.hadoop.conf.*;
import com.dataliance.hbase.table.vo.*;
import java.util.*;
import java.io.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import com.dataliance.service.util.*;
import com.dataliance.analysis.data.assess.vo.*;
import com.dataliance.hbase.query.*;
import com.dataliance.util.*;

public class AssessManager extends Query<AssessRecod>
{
    private static final byte[] family;
    private static final byte[] qualifier;
    private static final long increment = 1L;
    private static final int ID_LEN = 8;
    public static final byte[] VERIFY;
    private ContentQuery contentQuery;
    private AssessAdpter assessAdpter;
    
    public AssessManager(final Configuration conf, final String contentName, final String categoryName, final String assessName) {
        super(conf, assessName);
        this.contentQuery = new ContentQuery(conf, contentName, categoryName);
        this.assessAdpter = new AssessAdpter(conf, assessName);
    }
    
    public List<AssessRecod> generate(final String category, final long limit) throws IOException {
        final List<ContentVO> contents = this.contentQuery.scan(category, limit);
        if (contents != null) {
            final List<AssessRecod> results = new ArrayList<AssessRecod>();
            final String id = this.getId(category, 1L);
            for (final ContentVO content : contents) {
                final AssessRecod accessRecod = new AssessRecod();
                accessRecod.setCategory(category);
                accessRecod.setId(id);
                accessRecod.setRowKey(CategoryUtil.fillCategory(category) + "|" + id + "|" + content.getUrl());
                accessRecod.setContent(content);
                results.add(accessRecod);
            }
            if (!results.isEmpty()) {
                this.assessAdpter.insert(results);
                return results;
            }
        }
        return null;
    }
    
    public void edit(final String rowKey, final String category, final String categoryVerify) throws IOException {
        if (category.equals(categoryVerify)) {
            throw new IOException("category = " + category + " equals categoryVerify = " + categoryVerify);
        }
        final AssessRecod assess = this.doGet(rowKey);
        if (assess != null) {
            assess.setCategoryVerify(categoryVerify);
            this.assessAdpter.insert(assess);
            this.contentQuery.verify(assess.getContent(), categoryVerify);
            return;
        }
        throw new IOException("rowKey = " + rowKey + " is not exist!");
    }
    
    public List<AssessRecod> getCurrent(final String category, final long limit) throws IOException {
        final String id = this.getId(category, 0L);
        if (Integer.parseInt(id) == 0) {
            return this.generate(category, limit);
        }
        return this.getAssessRecods(category, id);
    }
    
    public List<AssessRecod> getAssessRecods(final String category, final String id) throws IOException {
        final String startRow = this.startRow(category, id);
        final String stopRow = this.stopRow(category, id);
        return this.scan(startRow, stopRow);
    }
    
    private String startRow(final String category, final String id) {
        return CategoryUtil.fillCategory(category) + "|" + id;
    }
    
    private String stopRow(final String category, final String id) {
        return CategoryUtil.fillCategory(category) + "|" + id + "~";
    }
    
    public float save(final String category, final String id) throws IOException {
        final List<AssessRecod> assess = this.getAssessRecods(category, id);
        if (assess != null) {
            float n = 0.0f;
            for (final AssessRecod a : assess) {
                if (!StringUtil.isEmpty(a.getCategoryVerify())) {
                    ++n;
                }
            }
            return (assess.size() - n) / assess.size();
        }
        return 0.0f;
    }
    
    @Override
    protected AssessRecod parse(final Result result) throws IOException {
        final AssessRecod assess = new AssessRecod();
        final String rowKey = Bytes.toString(result.getRow());
        assess.setRowKey(rowKey);
        final String[] vs = Constants.split(rowKey, 3);
        assess.setCategory(CategoryUtil.parseCategory(vs[0]));
        assess.setId(vs[1]);
        assess.setCategoryVerify(this.getValue(result.getValue(AssessManager.family, AssessManager.VERIFY)));
        final ContentVO con = new ContentVO();
        if (vs.length == 3) {
            con.setUrl(vs[2]);
            con.setRowKey(vs[2]);
        }
        con.setTitle(this.getValue(result.getValue(AssessManager.family, Constants.QUALIFIER_TITLE)));
        con.setContent(this.getValue(result.getValue(AssessManager.family, Constants.QUALIFIER_CONTENT)));
        con.setCategoryText(this.getValue(result.getValue(AssessManager.family, Constants.QUALIFIER_CATEGORY_TEXT)));
        con.setCategoryUrl(this.getValue(result.getValue(AssessManager.family, Constants.QUALIFIER_CATEGORY_URL)));
        con.setCategoryVerify(this.getValue(result.getValue(AssessManager.family, Constants.QUALIFIER_CATEGORY_VERIFY)));
        assess.setContent(con);
        return assess;
    }
    
    private String getValue(final byte[] b) {
        return (b != null) ? Bytes.toString(b) : null;
    }
    
    private String getId(final String category, final long increment) throws IOException {
        final long id = this.assessAdpter.increment(category, AssessManager.family, AssessManager.qualifier, increment);
        return NumFormat.prefixByZero(id, 8);
    }
    
    public static void main(final String[] args) {
        System.out.println(CategoryUtil.fillCategory("3000") + "|" + "00000100" + "~");
    }
    
    static {
        family = Bytes.toBytes("I");
        qualifier = Bytes.toBytes("Q");
        VERIFY = Bytes.toBytes("verify");
    }
}
