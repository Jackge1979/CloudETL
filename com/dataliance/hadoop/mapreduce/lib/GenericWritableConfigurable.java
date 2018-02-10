package com.dataliance.hadoop.mapreduce.lib;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import java.io.*;

public abstract class GenericWritableConfigurable extends Configured implements Writable, Configurable
{
    private static final String NOT_SET;
    private String type;
    private Writable instance;
    
    public GenericWritableConfigurable() {
        this.type = GenericWritableConfigurable.NOT_SET;
    }
    
    public void set(final Writable obj) {
        this.instance = obj;
        this.type = this.instance.getClass().getName();
    }
    
    public Writable get() {
        return this.instance;
    }
    
    public String toString() {
        return "GW[" + ((this.instance != null) ? ("class=" + this.instance.getClass().getName() + ",value=" + this.instance.toString()) : "(null)") + "]";
    }
    
    public void write(final DataOutput out) throws IOException {
        if (this.type == GenericWritableConfigurable.NOT_SET || this.instance == null) {
            throw new IOException("The GenericWritable has NOT been set correctly. type=" + this.type + ", instance=" + this.instance);
        }
        Text.writeString(out, this.type);
        this.instance.write(out);
    }
    
    public void readFields(final DataInput in) throws IOException {
        this.type = Text.readString(in);
        try {
            this.set((Writable)Class.forName(this.type).newInstance());
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Cannot initialize the class: " + this.type);
        }
        final Writable w = this.get();
        if (w instanceof Configurable) {
            ((Configurable)w).setConf(this.getConf());
        }
        w.readFields(in);
    }
    
    static {
        NOT_SET = null;
    }
}
