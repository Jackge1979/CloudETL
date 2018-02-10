package com.dataliance.hbase.vo;

import org.apache.hadoop.io.*;
import java.io.*;
import java.util.*;

public class HbExportVO implements Writable, Iterable<String>
{
    private static byte VERSION;
    private String tableName;
    private Collection<String> families;
    
    public HbExportVO() {
        this.families = new LinkedList<String>();
    }
    
    public String getTableName() {
        return this.tableName;
    }
    
    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }
    
    public void addFamily(final String family) {
        this.families.add(family);
    }
    
    public void readFields(final DataInput in) throws IOException {
        final byte v = in.readByte();
        if (v == HbExportVO.VERSION) {
            this.tableName = Text.readString(in);
            for (int size = in.readInt(), i = 0; i < size; ++i) {
                this.families.add(Text.readString(in));
            }
        }
    }
    
    public void write(final DataOutput out) throws IOException {
        out.writeByte(HbExportVO.VERSION);
        Text.writeString(out, this.tableName);
        out.writeInt(this.families.size());
        for (final String family : this.families) {
            Text.writeString(out, family);
        }
    }
    
    public Iterator<String> iterator() {
        return this.families.iterator();
    }
    
    static {
        HbExportVO.VERSION = 1;
    }
}
