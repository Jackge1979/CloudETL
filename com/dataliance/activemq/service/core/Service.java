package com.dataliance.activemq.service.core;

import java.util.*;
import org.apache.commons.logging.*;

public abstract class Service implements IService
{
    public static Log LOG;
    
    @Override
    public abstract void serve(final Map<String, String> p0) throws ServiceRunningException;
    
    @Override
    public abstract void hookOnExit();
    
    protected boolean sendMessage(final String serviceName, final Map<String, String> argnames2values) {
        final boolean sendOk = MessageSender.get().sendMessage(serviceName, argnames2values);
        MessageSender.get().shutdown();
        return sendOk;
    }
    
    static {
        Service.LOG = LogFactory.getLog(Service.class.getCanonicalName());
    }
}
