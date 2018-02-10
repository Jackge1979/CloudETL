package com.dataliance.etl.inject.shell;

import com.dataliance.etl.inject.local.*;
import com.dataliance.util.*;

import java.io.*;
import org.slf4j.*;

public abstract class AbstractShell implements Shell
{
    private static final Logger LOG;
    private PrintStream out;
    private PrintStream err;
    
    public AbstractShell() {
        this(System.out, System.err);
    }
    
    public AbstractShell(final PrintStream out, final PrintStream err) {
        this.out = out;
        this.err = err;
    }
    
    @Override
    public void copy(final String dest, final String... srcs) throws IOException {
        final Executor exe = this.initExecutor("cp");
        exe.addArgument("-rf");
        exe.addArgument(srcs);
        exe.addArgument(dest);
        this.execute(exe);
    }
    
    @Override
    public void move(final String dest, final String... srcs) throws IOException {
        final Executor exe = this.initExecutor("mv");
        exe.addArgument(srcs);
        exe.addArgument(dest);
        this.execute(exe);
    }
    
    @Override
    public void scp(final String host, final String userName, final String dest, final String... src) throws IOException {
        final Executor exe = this.initExecutor("scp");
        exe.addArgument("-r");
        exe.addArgument(src);
        if (!StringUtil.isEmpty(userName)) {
            exe.addArgument(userName + "@" + host + ":" + dest);
        }
        else {
            exe.addArgument(host + ":" + dest);
        }
        this.execute(exe);
    }
    
    @Override
    public void mkdir(final String dir) throws IOException {
        final Executor exe = this.initExecutor("mkdir");
        exe.addArgument("-p");
        exe.addArgument(dir);
        this.execute(exe);
    }
    
    @Override
    public InputStream cat(final String src) throws IOException {
        final Executor exe = this.initExecutor("cat");
        exe.addArgument(src);
        exe.execute();
        new ProcessConsole(exe.getErrorStream(), "ERR", this.err).start();
        return exe.getInputStream();
    }
    
    @Override
    public void copy(final boolean wait, final String dest, final String... srcs) throws IOException {
        final Executor exe = this.initExecutor("cp");
        exe.addArgument("-rf");
        exe.addArgument(srcs);
        exe.addArgument(dest);
        this.execute(exe, wait);
    }
    
    @Override
    public void move(final boolean wait, final String dest, final String... srcs) throws IOException {
        final Executor exe = this.initExecutor("mv");
        exe.addArgument(srcs);
        exe.addArgument(dest);
        this.execute(exe, wait);
    }
    
    @Override
    public void scp(final boolean wait, final String host, final String userName, final String dest, final String... src) throws IOException {
        final Executor exe = this.initExecutor("scp");
        exe.addArgument("-r");
        exe.addArgument(src);
        if (!StringUtil.isEmpty(userName)) {
            exe.addArgument(userName + "@" + host + ":" + dest);
        }
        else {
            exe.addArgument(host + ":" + dest);
        }
        this.execute(exe, wait);
    }
    
    @Override
    public void mkdir(final boolean wait, final String dir) throws IOException {
        final Executor exe = this.initExecutor("mkdir");
        exe.addArgument("-p");
        exe.addArgument(dir);
        this.execute(exe, wait);
    }
    
    @Override
    public InputStream cat(final boolean wait, final String src) throws IOException {
        final Executor exe = this.initExecutor("cat");
        exe.addArgument(src);
        exe.execute();
        new ProcessConsole(exe.getErrorStream(), "ERR", this.err).start();
        return exe.getInputStream();
    }
    
    @Override
    public InputStream exeCommand(final boolean wait, final String command, final String... args) throws IOException {
        final Executor exe = this.initExecutor(command);
        exe.addArgument(args);
        return this.execute(exe, wait);
    }
    
    protected abstract Executor initExecutor(final String p0);
    
    protected InputStream execute(final Executor exe) throws IOException {
        return this.execute(exe, false);
    }
    
    protected InputStream execute(final Executor exe, final boolean wait) throws IOException {
        exe.execute();
        final Thread errThread = new ProcessConsole(exe.getErrorStream(), "ERR", this.err);
        errThread.start();
        if (wait) {
            while (errThread.isAlive()) {
                try {
                    Thread.sleep(1L);
                }
                catch (InterruptedException e) {
                    AbstractShell.LOG.info(e.getMessage());
                }
            }
        }
        return exe.getInputStream();
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)AbstractShell.class);
    }
}
