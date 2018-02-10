package com.dataliance.hbase.delete;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.client.*;
import java.io.*;
import org.apache.hadoop.hbase.util.*;

import com.dataliance.hbase.*;

import java.util.*;

public class HbaseDelete extends HbaseOption
{
    public HbaseDelete(final Configuration conf, final String tableName) {
        super(conf, tableName);
    }
    
    public void delete(final byte[] rowKey) throws IOException {
        final Delete delete = new Delete(rowKey);
        final HTableInterface htable = this.getHTable();
        try {
            htable.delete(delete);
        }
        finally {
            this.release(htable);
        }
    }
    
    public void delete(final String rowKey) throws IOException {
        this.delete(Bytes.toBytes(rowKey));
    }
    
    public void delete(final List<String> rows) throws IOException {
        final List<Delete> deletes = new ArrayList<Delete>();
        for (final String row : rows) {
            deletes.add(new Delete(Bytes.toBytes(row)));
        }
        final HTableInterface htable = this.getHTable();
        try {
            htable.delete((List)deletes);
        }
        finally {
            this.release(htable);
        }
    }
}
