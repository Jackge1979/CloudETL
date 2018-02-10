package com.dataliance.etl.inject.local;

import java.util.*;

import com.dataliance.util.*;

import java.io.*;

public class Executor
{
    private ArrayList<String> args;
    private Process process;
    private ProcessKiller processKiller;
    protected InputStream inputStream;
    protected InputStream errorStream;
    protected OutputStream outputStream;
    
    public Executor() {
        this.args = new ArrayList<String>();
        this.process = null;
        this.processKiller = null;
        this.inputStream = null;
        this.errorStream = null;
        this.outputStream = null;
    }
    
    public Executor(final String commond) {
        this.args = new ArrayList<String>();
        this.process = null;
        this.processKiller = null;
        this.inputStream = null;
        this.errorStream = null;
        this.outputStream = null;
        if (!StringUtil.isEmpty(commond)) {
            this.args.add(commond);
        }
    }
    
    public ArrayList<String> getArgs() {
        return this.args;
    }
    
    public void addArgument(final String... args) {
        if (args != null && args.length > 0) {
            this.args.addAll(Arrays.asList(args));
        }
    }
    
    public void addArgument(final Collection<String> args) {
        if (args != null && args.size() > 0) {
            this.args.addAll(args);
        }
    }
    
    public void execute() throws IOException {
        final String[] cmd = this.args.toArray(new String[this.args.size()]);
        final Runtime runtime = Runtime.getRuntime();
        this.process = runtime.exec(cmd);
        runtime.addShutdownHook(new ProcessKiller(this.process));
        this.inputStream = this.process.getInputStream();
        this.outputStream = this.process.getOutputStream();
        this.errorStream = this.process.getErrorStream();
    }
    
    public InputStream getInputStream() {
        return this.inputStream;
    }
    
    public OutputStream getOutputStream() {
        return this.outputStream;
    }
    
    public InputStream getErrorStream() {
        return this.errorStream;
    }
    
    public void destroy() {
        if (this.inputStream != null) {
            try {
                this.inputStream.close();
            }
            catch (Throwable t) {}
            this.inputStream = null;
        }
        if (this.outputStream != null) {
            try {
                this.outputStream.close();
            }
            catch (Throwable t2) {}
            this.outputStream = null;
        }
        if (this.errorStream != null) {
            try {
                this.errorStream.close();
            }
            catch (Throwable t3) {}
            this.errorStream = null;
        }
        if (this.process != null) {
            this.process.destroy();
            this.process = null;
        }
        if (this.processKiller != null) {
            final Runtime runtime = Runtime.getRuntime();
            runtime.removeShutdownHook(this.processKiller);
            this.processKiller = null;
        }
    }
    
    public Process getProcess() {
        return this.process;
    }
}
