package com.dataliance.etl.inject.etl.vo;

import org.apache.hadoop.io.*;
import java.util.*;

public interface Data extends Writable
{
    String getSrc();
    
    boolean isDir();
    
    byte getStatus();
    
    void setStatus(final byte p0);
    
    Set<String> getParseHosts();
    
    String getDataHost();
    
    void setDatahost(final String p0);
    
    void addParseHost(final String p0);
    
    List<Data> getChildren();
    
    long length();
    
    String getDestDir();
}
