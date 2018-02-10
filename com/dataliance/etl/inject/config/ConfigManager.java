package com.dataliance.etl.inject.config;

import org.apache.hadoop.conf.*;
import java.io.*;
import java.util.*;
import org.slf4j.*;

import com.dataliance.util.*;

public class ConfigManager
{
    private static final Logger LOG;
    public static final String TYPE_COMPRESS = "compress";
    public static final String TYPE_FAIR_SCHEDULER = "scheduler";
    public static final String DA_CONFIG_FILES = "com.DA.config.files";
    private Configuration conf;
    
    public ConfigManager(final Configuration conf) throws IOException {
        this.conf = conf;
    }
    
    public Map<String, DAConfig> getAll() {
        final Map<String, DAConfig> configs = new HashMap<String, DAConfig>();
        for (final String file : this.getResources()) {
            final DAConfig conf = this.getConfig(file);
            configs.put(file, conf);
        }
        return configs;
    }
    
    public void addResource(final String resource) {
        final String resources = this.conf.get("com.DA.config.files");
        if (StringUtil.isEmpty(resources)) {
            this.conf.set("com.DA.config.files", resource);
        }
        else {
            this.conf.set("com.DA.config.files", resources + "," + resource);
        }
    }
    
    public Collection<String> getResources() {
        return (Collection<String>)this.conf.getStringCollection("com.DA.config.files");
    }
    
    public DAConfig getConf(final String resource) {
        return this.getConfig(resource);
    }
    
    public DAConfig getConf(final String resource, final String type) {
        if (type.equals("compress")) {
            return new CompressConf(this.conf, resource);
        }
        if (type.equals("scheduler")) {
            return new SchedulerConf(this.conf, resource);
        }
        return this.getConfig(resource);
    }
    
    private DAConf getConfig(final String resource) {
        return new DAConf(this.conf, resource);
    }
    
    public void apply() throws Exception {
        for (final DAConfig config : this.getAll().values()) {
            config.apply();
        }
    }
    
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        conf.addResource("/home/qiaolong/core-site.xml");
        System.out.println(conf.get("fs.default.name"));
        System.out.println(conf.getResource("/home/qiaolong/core-site.xml"));
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)ConfigManager.class);
    }
}
