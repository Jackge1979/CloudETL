package com.dataliance.hadoop.vo;

import org.apache.hadoop.io.*;
import java.io.*;

public class FileInfo implements WritableComparable<FileInfo>
{
    private String path;
    
    public String getPath() {
        return this.path;
    }
    
    public void setPath(final String path) {
        this.path = path;
    }
    
    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + ((this.path == null) ? 0 : this.path.hashCode());
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
        final FileInfo other = (FileInfo)obj;
        if (this.path == null) {
            if (other.path != null) {
                return false;
            }
        }
        else if (!this.path.equals(other.path)) {
            return false;
        }
        return true;
    }
    
    public void readFields(final DataInput in) throws IOException {
        this.path = WritableUtils.readString(in);
    }
    
    public void write(final DataOutput out) throws IOException {
        WritableUtils.writeString(out, this.path);
    }
    
    public int compareTo(final FileInfo other) {
        if (this.path == null) {
            return -1;
        }
        if (other == null || other.path == null) {
            return 1;
        }
        return this.path.compareTo(other.path);
    }
}
