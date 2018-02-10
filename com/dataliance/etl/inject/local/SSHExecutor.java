package com.dataliance.etl.inject.local;

import java.util.*;
import ch.ethz.ssh2.*;
import java.io.*;

public class SSHExecutor extends Executor
{
    private Connection conn;
    
    public SSHExecutor(final Connection conn, final String commond) {
        super(commond);
        this.conn = conn;
    }
    
    private String parseCommond() {
        final StringBuffer sb = new StringBuffer();
        for (final String arg : this.getArgs()) {
            sb.append(arg).append(" ");
        }
        return sb.toString();
    }
    
    @Override
    public void execute() throws IOException {
        final Session session = this.conn.openSession();
        session.execCommand(this.parseCommond());
        this.inputStream = session.getStdout();
        this.errorStream = session.getStderr();
        this.outputStream = session.getStdin();
    }
}
