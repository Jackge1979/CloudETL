package com.dataliance.etl.workflow.process;

import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
import com.google.common.collect.*;
import org.slf4j.*;

public class Parameters
{
    private static final Logger log;
    private Map<String, String> params;
    
    public Parameters() {
        this.params = new HashMap<String, String>();
    }
    
    public Parameters(final String serializedString) throws IOException {
        this(parseParams(serializedString));
    }
    
    protected Parameters(final Map<String, String> params) {
        this.params = new HashMap<String, String>();
        this.params = params;
    }
    
    public String get(final String key) {
        return this.params.get(key);
    }
    
    public String get(final String key, final String defaultValue) {
        final String ret = this.params.get(key);
        return (ret == null) ? defaultValue : ret;
    }
    
    public void set(final String key, final String value) {
        this.params.put(key, value);
    }
    
    public int getInt(final String key, final int defaultValue) {
        final String ret = this.params.get(key);
        return (ret == null) ? defaultValue : Integer.parseInt(ret);
    }
    
    @Override
    public String toString() {
        final Configuration conf = new Configuration();
        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        final DefaultStringifier<Map<String, String>> mapStringifier = (DefaultStringifier<Map<String, String>>)new DefaultStringifier(conf, GenericsUtil.getClass((Object)this.params));
        try {
            return mapStringifier.toString((Object)this.params);
        }
        catch (IOException e) {
            Parameters.log.info("Encountered IOException while deserializing returning empty string", (Throwable)e);
            return "";
        }
    }
    
    public String print() {
        return this.params.toString();
    }
    
    public static Map<String, String> parseParams(final String serializedString) throws IOException {
        final Configuration conf = new Configuration();
        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        final Map<String, String> params = (Map<String, String>)Maps.newHashMap();
        final DefaultStringifier<Map<String, String>> mapStringifier = (DefaultStringifier<Map<String, String>>)new DefaultStringifier(conf, GenericsUtil.getClass((Object)params));
        return (Map<String, String>)mapStringifier.fromString(serializedString);
    }
    
    public static void main(final String[] args) {
    }
    
    static {
        log = LoggerFactory.getLogger((Class)Parameters.class);
    }
}
