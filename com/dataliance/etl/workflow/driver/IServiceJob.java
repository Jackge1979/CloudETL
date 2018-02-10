package com.dataliance.etl.workflow.driver;

import org.apache.hadoop.conf.*;
import java.util.*;

public interface IServiceJob extends Configurable
{
    int run() throws Exception;
    
    void setJobInfo(final Map<String, String> p0);
    
    Map<String, String> getJobInfo();
}
