package com.dataliance.etl.inject.config;

import java.io.*;
import org.apache.hadoop.conf.*;

import com.dataliance.util.*;
/*
 * 配置Hadoop数据源
 */
public class HadoopSource extends Configured
{
    public static final String HOME = "com.da.home.hadoop";
    public static final String MASTER = "com.da.host.master";
    public static final String AUTH_USE_PASSWORD = "com.da.auth.use.password";
    public static final String USER = "com.da.host.master.user";
    public static final String PASSWORD = "com.da.host.master.password";
    public static final String PRI_KEY_FILE = "com.da.pri.key.file";
    private String master;
    private String home;
    private boolean usePassword;
    private String user;
    private String password;
    private File priKeyFile;
    
    public HadoopSource(final Configuration conf) {
        super(conf);
        this.master = conf.get("com.da.host.master", "da");
        this.home = conf.get("com.da.home.hadoop", "/opt/hadoop");
        this.usePassword = conf.getBoolean("com.da.auth.use.password", false);
        this.user = conf.get("com.da.host.master.user", "demo");
        this.password = conf.get("com.da.host.master.password", "demo");
        this.priKeyFile = new File(conf.get("com.da.pri.key.file", StreamUtil.getPriKeyPath()));
    }
    
    public String getMaster() {
        return this.master;
    }
    
    public void setMaster(final String master) {
        this.master = master;
    }
    
    public String getHome() {
        return this.home;
    }
    
    public void setHome(final String home) {
        this.home = home;
    }
    
    public boolean isUsePassword() {
        return this.usePassword;
    }
    
    public void setUsePassword(final boolean usePassword) {
        this.usePassword = usePassword;
    }
    
    public String getUser() {
        return this.user;
    }
    
    public void setUser(final String user) {
        this.user = user;
    }
    
    public String getPassword() {
        return this.password;
    }
    
    public void setPassword(final String password) {
        this.password = password;
    }
    
    public File getPriKeyFile() {
        return this.priKeyFile;
    }
    
    public void setPriKeyFile(final File priKeyFile) {
        this.priKeyFile = priKeyFile;
    }
}
