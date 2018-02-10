package com.dataliance.service.util;

import java.net.*;
import java.io.*;
import java.lang.management.*;

public class SystemUtil
{
    private static String pid;
    
    public static final String getHostName() {
        String hostName = "unknown-host";
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return hostName;
    }
    
    public static final String getCurrentPath() {
        return new File("").getAbsolutePath();
    }
    
    public static String getPID() {
        if (null == SystemUtil.pid) {
            final String processName = ManagementFactory.getRuntimeMXBean().getName();
            SystemUtil.pid = processName.split("@")[0];
        }
        return SystemUtil.pid;
    }
    
    static {
        SystemUtil.pid = null;
    }
}
