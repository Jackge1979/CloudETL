package com.dataliance.jetty;

import org.apache.hadoop.conf.*;

public abstract class FilterInitializer
{
    public abstract void initFilter(final FilterContainer p0, final Configuration p1);
}
