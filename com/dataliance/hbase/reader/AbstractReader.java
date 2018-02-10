package com.dataliance.hbase.reader;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.util.*;
import java.io.*;

import com.dataliance.hbase.*;
import com.dataliance.hbase.util.*;
import com.dataliance.util.*;

import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.client.*;
import java.lang.reflect.*;
import java.util.*;

public abstract class AbstractReader<V> implements Reader<V>
{
    public static final String METHOD_SET = "set";
    public static final String METHOD_GET = "get|is";
    private HTableFactory htableFactory;
    private Configuration conf;
    private Set<byte[]> skipRow;
    protected Class<V> vClass;
    private Filter filter;
    
    public Filter getFilter() {
        return this.filter;
    }
    
    public void setFilter(final Filter filter) {
        this.filter = filter;
    }
    
    protected AbstractReader(final Configuration conf, final Class<V> vClass) {
        this.skipRow = new HashSet<byte[]>();
        this.conf = conf;
        this.htableFactory = HTableFactory.getHTableFactory(conf);
        this.vClass = vClass;
    }
    
    protected void addSkipRow(final String rowKey) {
        this.skipRow.add(Bytes.toBytes(rowKey));
    }
    
    private boolean skip(final ResultScanner rs, final long rows) throws IOException {
        if (rows <= 0L) {
            return true;
        }
        long skip;
        for (skip = 0L; skip < rows && rs.next() != null; ++skip) {}
        return skip == rows;
    }
    
    @Override
    public List<V> goPage(final String tableName, final String startRow, final long currentPage, final long goPage, final long limit) throws Exception {
        if (goPage <= 1L) {
            return this.read(tableName, null, limit);
        }
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            if (goPage > currentPage) {
                final long pages = goPage - currentPage;
                final Scan scan = new Scan();
                long rows = (pages - 1L) * limit;
                if (!StringUtil.isEmpty(startRow)) {
                    scan.setStartRow(Bytes.toBytes(startRow));
                    ++rows;
                }
                final ResultScanner rs = htable.getScanner(scan);
                if (this.skip(rs, rows)) {
                    return this.read(rs, limit);
                }
            }
            else {
                final Scan scan2 = new Scan();
                final ResultScanner rs2 = htable.getScanner(scan2);
                if (this.skip(rs2, limit * (goPage - 1L))) {
                    return this.read(rs2, limit);
                }
            }
        }
        finally {
            this.htableFactory.release(htable);
        }
        return null;
    }
    
    @Override
    public List<V> query(final String tableName, final String startRow, final String prefix, final long currentPage, final long goPage, final long limit) throws Exception {
        if (goPage <= 0L) {
            return this.query(tableName, prefix, startRow, limit);
        }
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            if (goPage > currentPage) {
                final long pages = goPage - currentPage;
                final Scan scan = new Scan();
                long rows = (pages - 1L) * limit;
                if (!StringUtil.isEmpty(startRow)) {
                    scan.setStartRow(Bytes.toBytes(startRow));
                    ++rows;
                }
                this.initPrefixFilter(scan, prefix);
                final ResultScanner rs = htable.getScanner(scan);
                if (this.skip(rs, rows)) {
                    return this.read(rs, limit);
                }
            }
            else {
                final Scan scan2 = new Scan();
                this.initPrefixFilter(scan2, prefix);
                final ResultScanner rs2 = htable.getScanner(scan2);
                if (this.skip(rs2, limit * (goPage - 1L))) {
                    return this.read(rs2, limit);
                }
            }
        }
        finally {
            this.htableFactory.release(htable);
        }
        return null;
    }
    
    private void initPrefixFilter(final Scan scan, final String prefix) {
        final FilterList fl = new FilterList();
        if (this.filter != null) {
            fl.addFilter(this.filter);
        }
        fl.addFilter((Filter)new PrefixFilter(Bytes.toBytes(prefix)));
        scan.setFilter((Filter)fl);
    }
    
    @Override
    public V get(final String tableName, final String rowKey) throws Exception {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            final Get get = new Get(Bytes.toBytes(rowKey));
            final Result result = htable.get(get);
            return this.parse(result);
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    @Override
    public List<V> query(final String tableName, final String prefix, final String startRow, long limit) throws Exception {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            final Scan scan = new Scan();
            long rows = 0L;
            if (!StringUtil.isEmpty(startRow)) {
                scan.setStartRow(Bytes.toBytes(startRow));
                ++rows;
            }
            this.initPrefixFilter(scan, prefix);
            limit = ((limit <= 0L) ? Long.MAX_VALUE : limit);
            final ResultScanner rs = htable.getScanner(scan);
            if (rs != null && this.skip(rs, rows)) {
                return this.read(rs, limit);
            }
            return null;
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    private List<V> read(final ResultScanner rs, final long limit) throws Exception {
        final List<V> results = new ArrayList<V>();
        int i = 0;
        while (results.size() < limit) {
            final Result result = rs.next();
            if (result == null) {
                break;
            }
            final V v = this.parse(result);
            if (v != null) {
                results.add(v);
            }
            ++i;
        }
        return results.isEmpty() ? null : results;
    }
    
    private List<V> read(final ResultScanner rs) throws Exception {
        final List<V> results = new ArrayList<V>();
        for (final Result re : rs) {
            if (re != null) {
                final V v = this.parse(re);
                if (v == null) {
                    continue;
                }
                results.add(v);
            }
        }
        return results.isEmpty() ? null : results;
    }
    
    @Override
    public List<V> read(final String tableName, final String startRow, long limit) throws Exception {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            final Scan scan = new Scan();
            long rows = 0L;
            if (!StringUtil.isEmpty(startRow)) {
                scan.setStartRow(Bytes.toBytes(startRow));
                ++rows;
            }
            if (this.filter != null) {
                scan.setFilter(this.filter);
            }
            limit = ((limit <= 0L) ? Long.MAX_VALUE : limit);
            final ResultScanner rs = htable.getScanner(scan);
            if (rs != null && this.skip(rs, rows)) {
                return this.read(rs, limit);
            }
            return null;
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    @Override
    public List<V> read(final String tableName, final String startRow, final String stopRow) throws Exception {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            final Scan scan = new Scan();
            if (!StringUtil.isEmpty(startRow)) {
                scan.setStartRow(Bytes.toBytes(startRow));
            }
            if (StringUtil.isEmpty(stopRow)) {
                scan.setStopRow(Bytes.toBytes(stopRow));
            }
            if (this.filter != null) {
                scan.setFilter(this.filter);
            }
            final ResultScanner rs = htable.getScanner(scan);
            if (rs != null) {
                return this.read(rs);
            }
            return null;
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    protected void setValue(final V v, final Method[] methods, final String field, final byte[] value) throws Exception {
        for (final Method method : methods) {
            final String name = this.toField(method.getName());
            if (name.equalsIgnoreCase(field)) {
                final Class<?>[] clazzes = method.getParameterTypes();
                if (clazzes != null && clazzes.length == 1) {
                    final Class<?> calzz = clazzes[0];
                    final Object o = BaseTypeUtil.getValue(calzz, value);
                    if (o != null) {
                        method.invoke(v, o);
                    }
                }
            }
        }
    }
    
    private String toField(String method) {
        method = method.replaceFirst("^set", "");
        final char v = method.charAt(0);
        return method.replaceAll("^" + v, String.valueOf(Character.toLowerCase(v)));
    }
    
    private V parse(final Result result) throws Exception {
        final V v = this.vClass.newInstance();
        final Method[] methods = this.vClass.getMethods();
        final byte[] rowKey = result.getRow();
        if (this.skipRow.contains(rowKey)) {
            return null;
        }
        this.setValue(v, methods, "rowKey", rowKey);
        for (final Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : result.getMap().entrySet()) {
            final byte[] family = entry.getKey();
            final String fam = Bytes.toString(family);
            final NavigableMap<byte[], NavigableMap<Long, byte[]>> value = entry.getValue();
            for (final Map.Entry<byte[], NavigableMap<Long, byte[]>> chEntry : value.entrySet()) {
                final byte[] qualifier = chEntry.getKey();
                final NavigableMap<Long, byte[]> chValue = chEntry.getValue();
                final String field = Bytes.toString(qualifier);
                this.setValue(v, methods, field, chValue.lastEntry().getValue());
            }
        }
        return v;
    }
    
    private long getTotal(final ResultScanner rs) throws IOException {
        long total = 0L;
        if (rs != null) {
            for (Result re = rs.next(); re != null; re = rs.next()) {
                ++total;
            }
        }
        return total;
    }
    
    @Override
    public long queryTotal(final String tableName, final String prefix) throws Exception {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            final Scan scan = new Scan();
            this.initPrefixFilter(scan, prefix);
            final ResultScanner rs = htable.getScanner(scan);
            return this.getTotal(rs);
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    @Override
    public long getTotal(final String tableName) throws IOException {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            final Scan scan = new Scan();
            final ResultScanner rs = htable.getScanner(scan);
            return this.getTotal(rs);
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    @Override
    public List<V> goPage(final String tableName, final String startRow, final String endRow, final long start, final long limit) throws Exception {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            final Scan scan = new Scan();
            if (!StringUtil.isEmpty(startRow)) {
                scan.setStartRow(Bytes.toBytes(startRow));
            }
            if (StringUtil.isEmpty(endRow)) {
                scan.setStopRow(Bytes.toBytes(endRow));
            }
            if (this.filter != null) {
                scan.setFilter(this.filter);
            }
            final ResultScanner rs = htable.getScanner(scan);
            if (rs != null) {
                this.skip(rs, start);
                return this.read(rs, limit);
            }
            return null;
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
    
    @Override
    public long queryTotal(final String tableName, final String startRow, final String endRow) throws Exception {
        final HTableInterface htable = this.htableFactory.getHTable(tableName);
        try {
            final Scan scan = new Scan();
            if (!StringUtil.isEmpty(startRow)) {
                scan.setStartRow(Bytes.toBytes(startRow));
            }
            if (StringUtil.isEmpty(endRow)) {
                scan.setStopRow(Bytes.toBytes(endRow));
            }
            if (this.filter != null) {
                scan.setFilter(this.filter);
            }
            final ResultScanner rs = htable.getScanner(scan);
            if (rs != null) {
                return this.getTotal(rs);
            }
            return 0L;
        }
        finally {
            this.htableFactory.release(htable);
        }
    }
}
