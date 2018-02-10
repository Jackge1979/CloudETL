package com.dataliance.etl.inject.etl.vo;

import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.util.*;

import java.io.*;

public class ImportVO implements Writable
{
    public static final byte STATUS_ALL = -2;
    public static final byte STATUS_WAITING = 0;
    public static final byte STATUS_RUNNING = 1;
    public static final byte STATUS_FINISH = 2;
    public static final byte STATUS_ERROR = -1;
    private static final byte VERSION = 1;
    private static final ImportVO nullVO;
    private String host;
    private String source;
    private int port;
    private String destDir;
    private String extendName;
    private long start;
    private long limit;
    private long length;
    private boolean split;
    private boolean isNull;
    private String programPath;
    
    public String getProgramPath() {
        return this.programPath;
    }
    
    public void setProgramPath(final String programPath) {
        this.programPath = programPath;
    }
    
    public ImportVO() {
        this.isNull = false;
    }
    
    public ImportVO(final boolean isNull) {
        this.isNull = false;
        this.isNull = isNull;
    }
    
    public boolean isNull() {
        return this.isNull;
    }
    
    public void setNull(final boolean isNull) {
        this.isNull = isNull;
    }
    
    public int getPort() {
        return this.port;
    }
    
    public void setPort(final int port) {
        this.port = port;
    }
    
    public String getHost() {
        return this.host;
    }
    
    public void setHost(final String host) {
        this.host = host;
    }
    
    public String getSource() {
        return this.source;
    }
    
    public void setSource(final String source) {
        this.source = source;
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
    
    public String getExtendName() {
        return this.extendName;
    }
    
    public void setExtendName(final String extendName) {
        this.extendName = extendName;
    }
    
    public boolean isSplit() {
        return this.split;
    }
    
    public void setSplit(final boolean split) {
        this.split = split;
    }
    
    public void write(final DataOutput out) throws IOException {
        out.writeByte(1);
        out.writeBoolean(this.isNull);
        WritableUtils.writeString(out, this.programPath);
        if (!this.isNull) {
            WritableUtils.writeString(out, this.host);
            WritableUtils.writeString(out, this.source);
            WritableUtils.writeString(out, this.destDir);
            WritableUtils.writeString(out, this.extendName);
            WritableUtils.writeVLong(out, this.start);
            WritableUtils.writeVLong(out, this.limit);
            WritableUtils.writeVLong(out, this.length);
            WritableUtils.writeVInt(out, this.port);
            out.writeBoolean(this.split);
        }
    }
    
    public void readFields(final DataInput in) throws IOException {
        final byte v = in.readByte();
        if (v == 1) {
            this.isNull = in.readBoolean();
            this.programPath = WritableUtils.readString(in);
            if (!this.isNull) {
                this.host = WritableUtils.readString(in);
                this.source = WritableUtils.readString(in);
                this.destDir = WritableUtils.readString(in);
                this.extendName = WritableUtils.readString(in);
                this.start = WritableUtils.readVLong(in);
                this.limit = WritableUtils.readVLong(in);
                this.length = WritableUtils.readVLong(in);
                this.port = WritableUtils.readVInt(in);
                this.split = in.readBoolean();
            }
            return;
        }
        throw new VersionException();
    }
    
    @Override
    public String toString() {
        return "[host=" + this.host + ",port=" + this.port + ",source=" + this.source + "]";
    }
    
    public static final ImportVO getNull() {
        return ImportVO.nullVO;
    }
    
    static {
        nullVO = new ImportVO(true);
    }
}
