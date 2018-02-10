package com.dataliance.etl.inject.etl.vo;

import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.util.*;

import java.util.*;
import java.io.*;

public class DataHost extends Host implements Writable
{
    private static final byte VERSION = 1;
    private String programPath;
    private List<DirSource> sources;
    
    public DataHost() {
        this.sources = new ArrayList<DirSource>();
    }
    
    public String getProgramPath() {
        return this.programPath;
    }
    
    public void setProgramPath(final String programPath) {
        this.programPath = programPath;
    }
    
    public List<DirSource> getSources() {
        return this.sources;
    }
    
    public void addSource(final DirSource source) {
        this.sources.add(source);
    }
    
    public void write(final DataOutput out) throws IOException {
        out.writeByte(1);
        WritableUtils.writeString(out, this.programPath);
        WritableUtils.writeString(out, this.getHost());
        WritableUtils.writeString(out, this.getPassword());
        WritableUtils.writeString(out, this.getUser());
        WritableUtils.writeVInt(out, this.getPort());
        WritableUtils.writeVInt(out, this.sources.size());
        for (final DirSource ds : this.sources) {
            ds.write(out);
        }
    }
    
    public void readFields(final DataInput in) throws IOException {
        final byte v = in.readByte();
        if (v == 1) {
            this.programPath = WritableUtils.readString(in);
            this.setHost(WritableUtils.readString(in));
            this.setPassword(WritableUtils.readString(in));
            this.setUser(WritableUtils.readString(in));
            this.setPort(WritableUtils.readVInt(in));
            for (int size = WritableUtils.readVInt(in), i = 0; i < size; ++i) {
                this.addSource(DirSource.read(in));
            }
            return;
        }
        throw new VersionException();
    }
}
