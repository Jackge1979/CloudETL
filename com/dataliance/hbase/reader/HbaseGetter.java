package com.dataliance.hbase.reader;

import org.apache.hadoop.hbase.client.*;
import java.util.*;

public interface HbaseGetter
{
    Object getValue(final String p0, final String p1, final String p2, final String p3, final Class p4);
    
    String getValue(final String p0, final String p1, final String p2, final String p3);
    
    void putValue(final String p0, final String p1, final String p2, final String p3, final byte[] p4);
    
    void pubValue(final String p0, final Put p1);
    
    void putValue(final String p0, final List<Put> p1);
}
