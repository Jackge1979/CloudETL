package com.dataliance.hadoop.mapreduce.lib;

import org.apache.hadoop.io.*;

public class DAWritable extends GenericWritableConfigurable
{
    public DAWritable() {
    }
    
    public DAWritable(final Writable instance) {
        this.set(instance);
    }
}
