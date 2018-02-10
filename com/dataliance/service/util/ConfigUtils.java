package com.dataliance.service.util;

import org.apache.hadoop.conf.*;

public class ConfigUtils
{
    private static final Configuration config;
    
    public static Configuration getConfig() {
        return ConfigUtils.config;
    }
    
    public static void main(final String[] args) {
    }
    
    static {
        (config = new Configuration()).addResource("bigdata-site.xml");
    }
}
