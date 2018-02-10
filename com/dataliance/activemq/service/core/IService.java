package com.dataliance.activemq.service.core;

import java.util.*;

public interface IService
{
    void serve(final Map<String, String> p0) throws ServiceRunningException;
    
    void hookOnExit();
}
