package com.dataliance.hbase.reader;

import java.util.*;
import java.io.*;

public interface Reader<V>
{
    List<V> read(final String p0, final String p1, final long p2) throws Exception;
    
    List<V> read(final String p0, final String p1, final String p2) throws Exception;
    
    List<V> query(final String p0, final String p1, final String p2, final long p3) throws Exception;
    
    List<V> query(final String p0, final String p1, final String p2, final long p3, final long p4, final long p5) throws Exception;
    
    V get(final String p0, final String p1) throws Exception;
    
    List<V> goPage(final String p0, final String p1, final long p2, final long p3, final long p4) throws Exception;
    
    long queryTotal(final String p0, final String p1) throws Exception;
    
    long getTotal(final String p0) throws IOException;
    
    List<V> goPage(final String p0, final String p1, final String p2, final long p3, final long p4) throws Exception;
    
    long queryTotal(final String p0, final String p1, final String p2) throws Exception;
}
