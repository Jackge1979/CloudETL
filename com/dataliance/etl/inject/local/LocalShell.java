package com.dataliance.etl.inject.local;

import java.io.*;

import com.dataliance.etl.inject.shell.*;

public class LocalShell extends AbstractShell
{
    public LocalShell() {
        super(System.out, System.err);
    }
    
    public LocalShell(final PrintStream out, final PrintStream err) {
        super(out, err);
    }
    
    @Override
    protected Executor initExecutor(final String commond) {
        return new Executor(commond);
    }
    
    @Override
    public void close() throws IOException {
    }
}
