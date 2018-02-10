package com.dataliance.util;

import org.apache.hadoop.conf.*;
import java.util.*;

public class DAConfigUtil
{
    private static Configuration conf;
    public static final String UUID_KEY = "DA.conf.uuid";
    
    private static void setUUID(final Configuration conf) {
        final UUID uuid = UUID.randomUUID();
        conf.set("DA.conf.uuid", uuid.toString());
    }
    
    public static String getUUID(final Configuration conf) {
        return conf.get("DA.conf.uuid");
    }
    
    public static Configuration create() {
        if (DAConfigUtil.conf == null) {
            setUUID(DAConfigUtil.conf = new Configuration());
            addDAResources(DAConfigUtil.conf);
        }
        return DAConfigUtil.conf;
    }
    
    public static Configuration create(final boolean addDAResources, final Properties DAProperties) {
        final Configuration conf = new Configuration();
        setUUID(conf);
        if (addDAResources) {
            addDAResources(conf);
        }
        for (final Map.Entry<Object, Object> e : DAProperties.entrySet()) {
            conf.set(e.getKey().toString(), e.getValue().toString());
        }
        return conf;
    }
    
    private static Configuration addDAResources(final Configuration conf) {
        conf.addResource("DA-default.xml");
        conf.addResource("DA-site.xml");
        conf.addResource("bigdata-site.xml");
        return conf;
    }
}
