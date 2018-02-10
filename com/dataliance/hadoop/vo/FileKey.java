package com.dataliance.hadoop.vo;

import org.apache.hadoop.io.*;

import com.dataliance.util.*;

import java.io.*;

public class FileKey implements WritableComparable<FileKey>
{
    private String path;
    private long pos;
    private String outPath;
    
    public String getPath() {
        return this.path;
    }
    
    public void setPath(final String path) {
        this.path = path;
    }
    
    public String getOutPath() {
        return this.outPath;
    }
    
    public void setOutPath(final String outPath) {
        this.outPath = outPath;
    }
    
    public long getPos() {
        return this.pos;
    }
    
    public void setPos(final long pos) {
        this.pos = pos;
    }
    
    public void write(final DataOutput out) throws IOException {
        Text.writeString(out, StringUtil.toString(this.path));
        out.writeLong(this.pos);
        Text.writeString(out, StringUtil.toString(this.outPath));
    }
    
    public void readFields(final DataInput in) throws IOException {
        this.path = Text.readString(in);
        this.pos = in.readLong();
        this.outPath = Text.readString(in);
    }
    
    public int compareTo(final FileKey other) {
        final long n = this.pos - other.pos;
        if (n == 0L) {
            return StringUtil.compare(this.path, other.path);
        }
        return (n > 0L) ? 1 : -1;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = 31 * result + ((this.path == null) ? 0 : this.path.hashCode());
        result = 31 * result + (int)(this.pos ^ this.pos >>> 32);
        return result;
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final FileKey other = (FileKey)obj;
        if (this.path == null) {
            if (other.path != null) {
                return false;
            }
        }
        else if (!this.path.equals(other.path)) {
            return false;
        }
        return this.pos == other.pos;
    }
    
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append(this.path);
        builder.append("|+|");
        builder.append(this.pos);
        return builder.toString();
    }
}
