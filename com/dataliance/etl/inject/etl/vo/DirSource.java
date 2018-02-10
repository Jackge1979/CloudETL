package com.dataliance.etl.inject.etl.vo;

import java.io.*;
import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.util.*;
/*
 * 配置目录
 */
public class DirSource implements Writable
{
    private static final byte VERSION = 1;
    private String src;
    private String dest;
    
    public String getSrc() {
        return this.src;
    }
    
    public void setSrc(final String src) {
        this.src = src;
    }
    
    public String getDest() {
        return this.dest;
    }
    
    public void setDest(final String dest) {
        this.dest = dest;
    }
    
    public static DirSource read(final DataInput in) throws IOException {
        final DirSource dirSource = new DirSource();
        dirSource.readFields(in);
        return dirSource;
    }
    
    public void write(final DataOutput out) throws IOException {
        out.writeByte(1);
        WritableUtils.writeString(out, this.src);
        WritableUtils.writeString(out, this.dest);
    }
    
    public void readFields(final DataInput in) throws IOException {
        final byte b = in.readByte();
        if (b == 1) {
            this.src = WritableUtils.readString(in);
            this.dest = WritableUtils.readString(in);
            return;
        }
        throw new VersionException();
    }
    
    @Override
    public String toString() {
        return "src = " + this.src + " dest = " + this.dest;
    }
}
