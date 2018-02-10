package com.dataliance.util;

import org.apache.hadoop.*;

public class VersionInfo
{
    private static Package myPackage;
    private static DAVersionAnnotation version;
    
    static Package getPackage() {
        return VersionInfo.myPackage;
    }
    
    public static String getVersion() {
        return (VersionInfo.version != null) ? VersionInfo.version.version() : "Unknown";
    }
    
    public static String getRevision() {
        return (VersionInfo.version != null) ? VersionInfo.version.revision() : "Unknown";
    }
    
    public static String getDate() {
        return (VersionInfo.version != null) ? VersionInfo.version.date() : "Unknown";
    }
    
    public static String getUser() {
        return (VersionInfo.version != null) ? VersionInfo.version.user() : "Unknown";
    }
    
    public static String getUrl() {
        return (VersionInfo.version != null) ? VersionInfo.version.url() : "Unknown";
    }
    
    public static String getSrcChecksum() {
        return (VersionInfo.version != null) ? VersionInfo.version.srcChecksum() : "Unknown";
    }
    
    public static String getBuildVersion() {
        return getVersion() + " from " + getRevision() + " by " + getUser() + " source checksum " + getSrcChecksum();
    }
    
    public static void main(final String[] args) {
        System.out.println("Hadoop " + getVersion());
        System.out.println("Subversion " + getUrl() + " -r " + getRevision());
        System.out.println("Compiled by " + getUser() + " on " + getDate());
        System.out.println("From source with checksum " + getSrcChecksum());
    }
    
    static {
        VersionInfo.myPackage = HadoopVersionAnnotation.class.getPackage();
        VersionInfo.version = VersionInfo.myPackage.getAnnotation(DAVersionAnnotation.class);
    }
}
