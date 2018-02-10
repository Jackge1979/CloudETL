package com.dataliance.etl.inject.ssh;

import ch.ethz.ssh2.*;

import java.io.*;

import com.dataliance.etl.inject.local.*;
import com.dataliance.etl.inject.shell.*;

public class SSHShell extends AbstractShell
{
    private Connection conn;
    
    public SSHShell(final Connection conn) {
        super(System.out, System.err);
        this.conn = conn;
    }
    
    public SSHShell(final Connection conn, final PrintStream out, final PrintStream err) {
        super(out, err);
        this.conn = conn;
    }
    
    @Override
    protected Executor initExecutor(final String commond) {
        return new SSHExecutor(this.conn, commond);
    }
    
    @Override
    public void close() throws IOException {
        if (this.conn != null) {
            this.conn.close();
        }
    }
}
