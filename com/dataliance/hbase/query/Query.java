package com.dataliance.hbase.query;

import com.dataliance.hbase.*;
import com.dataliance.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.*;
import java.io.*;
import org.apache.hadoop.hbase.client.*;
import java.util.*;
import org.apache.hadoop.hbase.filter.*;

public abstract class Query<V> extends Configured implements Configurable
{
    public static final String MAX_CHAR = "~";
    public static final long UNLIMIT = -1L;
    public static final long NOSKIP = -1L;
    private HTableFactory htableFactory;
    private String tableName;
    private DelayUtil queryDelay;
    private DelayUtil connDelay;
    
    public Query(final Configuration conf, final String tableName) {
        super(conf);
        this.htableFactory = HTableFactory.getHTableFactory(conf);
        this.tableName = tableName;
    }
    
    public DelayUtil getQueryDelay() {
        return this.queryDelay;
    }
    
    public void setQueryDelay(final DelayUtil queryDelay) {
        this.queryDelay = queryDelay;
    }
    
    public DelayUtil getConnDelay() {
        return this.connDelay;
    }
    
    public void setConnDelay(final DelayUtil connDelay) {
        this.connDelay = connDelay;
    }
    
    public V doGet(final String rowKey) throws IOException {
        return this.doGet(Bytes.toBytes(rowKey));
    }
    
    public V doGet(final byte[] rowKey) throws IOException {
        final Get get = new Get(rowKey);
        final HTableInterface htable = this.htableFactory.getHTable(this.tableName);
        try {
            final Result result = htable.get(get);
            if (result != null) {
                return this.parse(result);
            }
            return null;
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    public List<V> scan(final byte[] startRow, final byte[] stopRow) throws IOException {
        return this.scan(startRow, stopRow, -1L);
    }
    
    public List<V> scan(final byte[] startRow, final byte[] stopRow, final long limit) throws IOException {
        return this.scan(startRow, stopRow, -1L, limit);
    }
    
    public List<V> scan(final byte[] startRow, final byte[] stopRow, final long skip, final long limit) throws IOException {
        final Scan scan = new Scan(startRow, stopRow);
        final Filter filter = this.getPageFilter(skip, limit);
        if (filter != null) {
            scan.setFilter(filter);
        }
        return this.scan(scan, skip);
    }
    
    public List<V> scan(final Scan scan, final long skip) throws IOException {
        if (this.connDelay != null) {
            this.connDelay.setStart(System.currentTimeMillis());
        }
        final HTableInterface htable = this.htableFactory.getHTable(this.tableName);
        if (this.connDelay != null) {
            this.connDelay.setEnd(System.currentTimeMillis());
        }
        try {
            if (this.queryDelay != null) {
                this.queryDelay.setStart(System.currentTimeMillis());
            }
            final ResultScanner rs = htable.getScanner(scan);
            if (this.queryDelay != null) {
                this.queryDelay.setEnd(System.currentTimeMillis());
            }
            final List<V> results = new ArrayList<V>();
            if (rs != null && this.skip(rs, skip)) {
                for (final Result result : rs) {
                    final V v = this.parse(result);
                    if (v != null) {
                        results.add(v);
                    }
                }
            }
            return results.isEmpty() ? null : results;
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    public List<V> scan(final Scan scan) throws IOException {
        return this.scan(scan, -1L);
    }
    
    public List<V> scan(final String startRow, final String stopRow) throws IOException {
        return this.scan(startRow, stopRow, -1L);
    }
    
    public List<V> scan(final String startRow, final String stopRow, final long limit) throws IOException {
        return this.scan(startRow, stopRow, -1L, limit);
    }
    
    public List<V> scan(final String startRow, final String endRow, final long skip, final long limit) throws IOException {
        return this.scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow), skip, limit);
    }
    
    protected abstract V parse(final Result p0) throws IOException;
    
    protected Filter getPageFilter(final long skip, final long limit) {
        if (limit >= 0L) {
            final long pageSize = (skip >= 0L) ? (limit + skip) : limit;
            final Filter filter = (Filter)new PageFilter(pageSize);
            return filter;
        }
        return null;
    }
    
    public long getTotal(final String startRow, final String endRow) throws IOException {
        return this.getTotal(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
    }
    
    public long getTotal(final byte[] startRow, final byte[] stopRow) throws IOException {
        final Scan scan = new Scan(startRow, stopRow);
        return this.getTotal(scan);
    }
    
    public long getTotal(final Scan scan) throws IOException {
        final HTableInterface htable = this.htableFactory.getHTable(this.tableName);
        long total = 0L;
        try {
            final ResultScanner rs = htable.getScanner(scan);
            if (rs != null) {
                while (rs.next() != null) {
                    ++total;
                }
            }
        }
        finally {
            this.htableFactory.release(htable);
        }
        return total;
    }
    
    public Result getResult(final Get get) throws IOException {
        final HTableInterface htable = this.htableFactory.getHTable(this.tableName);
        try {
            return htable.get(get);
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    public Result getResult(final byte[] rowKey) throws IOException {
        return this.getResult(new Get(rowKey));
    }
    
    public Result getResult(final String rowKey) throws IOException {
        return this.getResult(Bytes.toBytes(rowKey));
    }
    
    private boolean skip(final ResultScanner rs, final long rows) throws IOException {
        if (rows <= 0L) {
            return true;
        }
        long skip;
        for (skip = 0L; skip < rows && rs.next() != null; ++skip) {}
        return skip == rows;
    }
}
