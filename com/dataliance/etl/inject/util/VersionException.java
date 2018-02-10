package com.dataliance.etl.inject.util;

import java.io.*;

public class VersionException extends IOException
{
    private static final long serialVersionUID = 1L;
    
    public VersionException() {
        super("Version is not matches!");
    }
    
    public VersionException(final byte old, final byte now) {
        super("Old vsersion = " + old + " now version = " + now + "! Version is not matches!");
    }
}
