package com.dataliance.etl.inject.shell;

import java.io.*;

public interface Shell
{
    public static final String SEPARATOR = "/";
    
    void copy(final String p0, final String... p1) throws IOException;
    
    void move(final String p0, final String... p1) throws IOException;
    
    void scp(final String p0, final String p1, final String p2, final String... p3) throws IOException;
    
    void mkdir(final String p0) throws IOException;
    
    InputStream cat(final String p0) throws IOException;
    
    void copy(final boolean p0, final String p1, final String... p2) throws IOException;
    
    void move(final boolean p0, final String p1, final String... p2) throws IOException;
    
    void scp(final boolean p0, final String p1, final String p2, final String p3, final String... p4) throws IOException;
    
    void mkdir(final boolean p0, final String p1) throws IOException;
    
    InputStream cat(final boolean p0, final String p1) throws IOException;
    
    InputStream exeCommand(final boolean p0, final String p1, final String... p2) throws IOException;
    
    void close() throws IOException;
}
