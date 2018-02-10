package com.dataliance.hbase;

import java.io.*;
import java.util.*;

public interface HbaseCrud
{
    void createTalbe(final String p0, final String[] p1) throws IOException;
    
    void dorpTable(final String p0) throws IOException;
    
    void insertRow(final String p0, final String p1, final String p2, final String p3, final Object p4) throws IOException;
    
    void deleteRow(final String p0, final String p1) throws IOException;
    
    void deleteRow(final String p0, final List<String> p1) throws IOException;
    
    void deleteCell(final String p0, final String p1, final String p2, final String p3) throws IOException;
    
    void updateCell(final String p0, final String p1, final String p2, final String p3, final Object p4) throws IOException;
    
    void deleteFamliy(final String p0, final String p1) throws IOException;
    
    void deleteFamliy(final String p0, final List<String> p1) throws IOException;
}
