package com.dataliance.etl.output.vo;

import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.util.*;

import java.io.*;

public class OutputVO implements Writable
{
    private static final byte VERSION = 1;
    private static final OutputVO nullVO;
    private String src;
    private String destDir;
    private long start;
    private long limit;
    private long length;
    private boolean split;
    private boolean isNull;
    private String programPath;
    
    public static final OutputVO getNull() {
        return OutputVO.nullVO;
    }
    
    public OutputVO() {
        this.isNull = false;
    }
    
    public OutputVO(final boolean isNull) {
        this.isNull = false;
        this.isNull = isNull;
    }
    
    public String getSrc() {
        return this.src;
    }
    
    public void setSrc(final String src) {
        this.src = src;
    }
    
    public String getDestDir() {
        return this.destDir;
    }
    
    public void setDestDir(final String destDir) {
        this.destDir = destDir;
    }
    
    public long getStart() {
        return this.start;
    }
    
    public void setStart(final long start) {
        this.start = start;
    }
    
    public long getLimit() {
        return this.limit;
    }
    
    public void setLimit(final long limit) {
        this.limit = limit;
    }
    
    public long getLength() {
        return this.length;
    }
    
    public void setLength(final long length) {
        this.length = length;
    }
    
    public boolean isSplit() {
        return this.split;
    }
    
    public void setSplit(final boolean split) {
        this.split = split;
    }
    
    public boolean isNull() {
        return this.isNull;
    }
    
    public void setNull(final boolean isNull) {
        this.isNull = isNull;
    }
    
    public String getProgramPath() {
        return this.programPath;
    }
    
    public void setProgramPath(final String programPath) {
        this.programPath = programPath;
    }
    
    public void write(final DataOutput out) throws IOException {
        out.writeByte(1);
        out.writeBoolean(this.isNull);
        WritableUtils.writeString(out, this.programPath);
        if (!this.isNull) {
            WritableUtils.writeString(out, this.src);
            WritableUtils.writeString(out, this.destDir);
            WritableUtils.writeVLong(out, this.start);
            WritableUtils.writeVLong(out, this.limit);
            WritableUtils.writeVLong(out, this.length);
            out.writeBoolean(this.split);
        }
    }
    
    public void readFields(final DataInput in) throws IOException {
        final byte v = in.readByte();
        if (v == 1) {
            this.isNull = in.readBoolean();
            this.programPath = WritableUtils.readString(in);
            if (!this.isNull) {
                this.src = WritableUtils.readString(in);
                this.destDir = WritableUtils.readString(in);
                this.start = WritableUtils.readVLong(in);
                this.limit = WritableUtils.readVLong(in);
                this.length = WritableUtils.readVLong(in);
                this.split = in.readBoolean();
            }
            return;
        }
        throw new VersionException();
    }
    
    static {
        nullVO = new OutputVO(true);
    }
}
