package com.dataliance.analysis.data.recommend;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import java.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;

public class UserBehaviorFeatureManager
{
    private MapFile.Reader mapReader;
    private FileSystem fs;
    private Configuration conf;
    
    public UserBehaviorFeatureManager(final Configuration conf, final String mapFilePath) {
        this.mapReader = null;
        this.fs = null;
        this.conf = null;
        try {
            this.fs = FileSystem.get(conf);
            this.mapReader = new MapFile.Reader(this.fs, mapFilePath, conf);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public Writable search(final String searchKey) {
        final Class<Writable> valueCls = (Class<Writable>)this.mapReader.getValueClass();
        final Writable value = (Writable)ReflectionUtils.newInstance((Class)valueCls, this.conf);
        Writable feature = null;
        try {
            final Text key = new Text(searchKey);
            feature = this.mapReader.get((WritableComparable)key, value);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return feature;
    }
    
    public void getSuggestUser() {
    }
    
    public static void main(final String[] args) {
        final String mapFilePath = "E:\\git-repository\\git\\bigdata-core\\data\\UserFeature\\20120214\\part-r-00000";
        final Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        final UserBehaviorFeatureManager join = new UserBehaviorFeatureManager(conf, mapFilePath);
        final String phoneNumber = "18603217286";
        final UserFeature userFeature = (UserFeature)join.search(phoneNumber);
        System.out.println(userFeature);
    }
}
