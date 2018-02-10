package com.dataliance.jetty;

import java.util.*;

public interface FilterContainer
{
    void addFilter(final String p0, final String p1, final Map<String, String> p2);
    
    void addGlobalFilter(final String p0, final String p1, final Map<String, String> p2);
}
