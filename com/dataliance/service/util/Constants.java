package com.dataliance.service.util;

import java.util.regex.*;
import org.apache.hadoop.hbase.util.*;

public class Constants
{
    public static final String CONFIG_FILENAME = "bigdata-site.xml";
    public static final String IMPORETER_WORKER_FILENAME = "importer_worker";
    public static final String COMPRESS_MAX_THREAD = "compress.max.thread";
    public static final String RAW_DATA_SPLIT = "@#\\$";
    public static final String ROW_KEY_SPLIT = "|";
    public static final String ROW_KEY_SPLIT_REGEX = "\\|";
    public static final String SPLIT = "|+|";
    public static final String SPLIT_REGEX = "\\|\\+\\|";
    public static final String HBASE_TABLE_TTL = "hbase.table.ttl";
    public static final String HBASE_TABLE_REGION_ENDKEY = "init.region.endkey";
    public static final String HBASE_TABLE_REGION_STARTKEY = "init.region.startkey";
    public static final String HBASE_TABLE_NUMREGIONS = "hbase.table.numregions";
    public static final String HBASE_TABLE_FAMILY = "hbase.table.family";
    public static final String HBASE_TABLE_QUALIFIER = "hbase.table.qualifier";
    public static final String HBASE_TABLE_CLASSIFIER_NAME = "hbase.table.classifier.name";
    public static final String HBASE_TABLE_CLASSIFIER_FAMILY_NAME = "hbase.table.classifier.family.name";
    public static final String HBASE_TABLE_CLASSIFIER_QULIFIER_NAME = "hbase.table.classifier.qualifier.name";
    public static final String HBASE_TABLE_CLASSIFIER_STATISTIC_NAME = "hbase.table.classifer.statistic.name";
    public static final String HBASE_TABLE_CLASSIFIER_STATISTIC_FAMILY_NAME = "hbase.table.classifer.statistic.family.name";
    public static final String HBASE_TABLE_CLASSIFIER_STATISTIC_QUALIFIER_NAME = "hbase.table.classifer.statistic.qualifier.name";
    public static final String HBASE_TABLE_URL_KEYWORD_NAME = "hbase.table.url.keyword.name";
    public static final String HBASE_TABLE_KEYWORD_NAME = "hbase.table.keyword.name";
    public static final byte[] QUALIFIER_TITLE;
    public static final byte[] QUALIFIER_CONTENT;
    public static final byte[] QUALIFIER_CATEGORY_TEXT;
    public static final byte[] QUALIFIER_CATEGORY_URL;
    public static final byte[] QUALIFIER_CATEGORY_VERIFY;
    public static final String AUTO_SEND_MESSAGE_TO_MQ = "auto.send.message.to.mq";
    public static final Pattern SPLIT_TOOL;
    
    public static final String[] split(final String value) {
        return split(value, 0);
    }
    
    public static final String[] split(final String value, final int limit) {
        return Constants.SPLIT_TOOL.split(value, limit);
    }
    
    public static void main(final String[] args) {
        final String value = "00005|http://www.baidu.com/kkkk|llll";
        System.out.println(split(value)[0]);
        System.out.println(split(value)[1]);
        System.out.println(split(value).length);
    }
    
    static {
        QUALIFIER_TITLE = Bytes.toBytes(WAP_CLASSIFIED.TITLE.name());
        QUALIFIER_CONTENT = Bytes.toBytes(WAP_CLASSIFIED.CONTENT.name());
        QUALIFIER_CATEGORY_TEXT = Bytes.toBytes(WAP_CLASSIFIED.CATEGORY_TEXT.name());
        QUALIFIER_CATEGORY_URL = Bytes.toBytes(WAP_CLASSIFIED.CATEGORY_URL.name());
        QUALIFIER_CATEGORY_VERIFY = Bytes.toBytes(WAP_CLASSIFIED.CATEGORY_VERIFY.name());
        SPLIT_TOOL = Pattern.compile("\\|");
    }
    
    public enum WAP_CLASSIFIED
    {
        URL, 
        TITLE, 
        CONTENT, 
        CATEGORY_TEXT, 
        CATEGORY_URL, 
        CATEGORY_VERIFY;
    }
}
