package com.dataliance.etl.workflow.process;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
import org.apache.commons.lang.*;
import java.io.*;
import org.slf4j.*;

public class SerializationUtil
{
    private static final Logger log;
    
    public static String serializeObject(final Object obj) {
        final Configuration conf = new Configuration();
        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        final DefaultStringifier<Object> mapStringifier = (DefaultStringifier<Object>)new DefaultStringifier(conf, GenericsUtil.getClass(obj));
        try {
            return mapStringifier.toString(obj);
        }
        catch (IOException e) {
            SerializationUtil.log.info("Encountered IOException while deserializing returning empty string", (Throwable)e);
            return "";
        }
    }
    
    public static Object deserializeObject(final String serializedString, final Class classes) throws IOException {
        final Configuration conf = new Configuration();
        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        final DefaultStringifier<Object> mapStringifier = (DefaultStringifier<Object>)new DefaultStringifier(conf, classes);
        return mapStringifier.fromString(serializedString);
    }
    
    public static String serialize(final Serializable obj) throws UnsupportedEncodingException {
        return new String(SerializationUtils.serialize(obj), "ISO-8859-1");
    }
    
    public static Object deserialize(final String serString) throws UnsupportedEncodingException {
        return SerializationUtils.deserialize(serString.getBytes("ISO-8859-1"));
    }
    
    static {
        log = LoggerFactory.getLogger((Class)SerializationUtil.class);
    }
}
