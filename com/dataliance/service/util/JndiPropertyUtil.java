package com.dataliance.service.util;

import org.apache.log4j.*;
import java.util.*;
import java.io.*;

public class JndiPropertyUtil
{
    private static final Logger LOG;
    public static final String DA_CONN_FACNAME = "service.qcf";
    public static final String JNDI_PROPERTY_FILE_NAME = "jndi.properties";
    private static Properties BASE_JNI_PROPERTIES;
    
    public static synchronized Properties getBaseJndiProperties() throws IOException {
        if (JndiPropertyUtil.BASE_JNI_PROPERTIES != null) {
            return JndiPropertyUtil.BASE_JNI_PROPERTIES;
        }
        JndiPropertyUtil.LOG.info((Object)"Load the jndi properties for the bigdata jms.");
        JndiPropertyUtil.BASE_JNI_PROPERTIES = new Properties();
        final InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("jndi.properties");
        JndiPropertyUtil.BASE_JNI_PROPERTIES.load(inputStream);
        return JndiPropertyUtil.BASE_JNI_PROPERTIES;
    }
    
    static {
        LOG = Logger.getLogger(JndiPropertyUtil.class.getCanonicalName());
        JndiPropertyUtil.BASE_JNI_PROPERTIES = null;
    }
    
    public enum SERVICE_QUEUE
    {
        DataImportService, 
        CrawlerService, 
        ClassiferService, 
        DummyService;
    }
    
    public enum SERVICE_RESULT_PATH
    {
        UNCLIASSIFIED_URL_PATH, 
        CRAWLED_SEGMENT_PATH;
    }
}
