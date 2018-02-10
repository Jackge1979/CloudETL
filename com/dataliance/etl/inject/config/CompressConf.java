package com.dataliance.etl.inject.config;

import org.apache.hadoop.conf.*;
import java.util.*;

class CompressConf extends DAConf
{
    private static final Set<String> keys;
    
    public CompressConf(final Configuration conf, final String resource) {
        super(conf, resource);
        this.setFilter(new Filter() {
            @Override
            public boolean accept(final String name, final String value) {
                return CompressConf.keys.contains(name);
            }
        });
    }
    
    @Override
    public void set(final String name, final String value) {
        if (CompressConf.keys.contains(name)) {
            super.set(name, value);
        }
    }
    
    static {
        (keys = new HashSet<String>()).add("io.compression.codecs");
        CompressConf.keys.add("mapred.compress.map.output");
        CompressConf.keys.add("mapred.map.output.compression.codec");
        CompressConf.keys.add("mapred.output.compress");
        CompressConf.keys.add("mapred.output.compression.codec");
    }
}
