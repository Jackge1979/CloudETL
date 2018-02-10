package com.dataliance.activemq.service.core;

import java.util.*;

public class DummyService extends Service
{
    @Override
    public void serve(final Map<String, String> argName2value) throws ServiceRunningException {
        System.err.println("DummyService server begin!");
        for (final Map.Entry<String, String> entry : argName2value.entrySet()) {
            System.err.println(entry.getKey() + ":" + entry.getValue());
        }
    }
    
    @Override
    public void hookOnExit() {
    }
}
