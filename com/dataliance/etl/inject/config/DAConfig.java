package com.dataliance.etl.inject.config;

import java.util.*;

public interface DAConfig extends Iterable<Map.Entry<String, String>>
{
    String get(final String p0);
    
    String delete(final String p0);
    
    void set(final String p0, final String p1);
    
    int size();
    
    void apply() throws Exception;
}
