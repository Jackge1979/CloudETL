package com.dataliance.activemq.service.core;

public class ServiceRunningException extends Exception
{
    private static final long serialVersionUID = 5777971725928536732L;
    
    public ServiceRunningException() {
    }
    
    public ServiceRunningException(final String message) {
        super(message);
    }
    
    public ServiceRunningException(final Throwable cause) {
        super(cause);
    }
    
    public ServiceRunningException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
