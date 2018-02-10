package com.dataliance.etl.inject.job.vo;

import org.apache.hadoop.io.*;
import java.util.*;
import java.io.*;

public class HostList implements Writable
{
    private List<Host> hosts;
    
    public HostList() {
        this.hosts = new ArrayList<Host>();
    }
    
    public List<Host> getHosts() {
        return this.hosts;
    }
    
    public void setHosts(final List<Host> hosts) {
        this.hosts = hosts;
    }
    
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.hosts.size());
        for (final Host host : this.hosts) {
            host.write(out);
        }
    }
    
    public void readFields(final DataInput in) throws IOException {
        for (int size = in.readInt(), i = 0; i < size; ++i) {
            this.hosts.add(Host.read(in));
        }
    }
}
