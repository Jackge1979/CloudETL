package com.dataliance.etl.inject.etl.vo;

import org.apache.hadoop.io.*;
import java.util.*;
/*
 *  配置数据源
 */
public interface DataSource extends Writable
{
    String getHost();
    
    int getPort();
    
    String getUser();
    
    String getPassword();
    
    String getProtocol();
    
    Map<String, Data> getDatas();
    
    void addData(final Data p0);
}
