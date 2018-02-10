package com.dataliance.etl.inject.ssh;

import ch.ethz.ssh2.*;

import com.dataliance.etl.inject.shell.*;
import com.dataliance.util.*;

import java.io.*;
import org.slf4j.*;

public class SSHClient implements Closeable
{
    private static final Logger LOG;
    private String user;
    private String host;
    private Connection conn;
    private SCPClient scpClient;
    private Shell shell;
    
    public SSHClient(final String host, final String user, final String password) throws IOException {
        this.user = user;
        this.host = host;
        (this.conn = new Connection(host)).connect();
        final boolean auth = this.conn.authenticateWithPassword(user, password);
        if (!auth) {
            throw new IOException("Username or password is invalided!");
        }
        this.init();
    }
    
    public SSHClient(final String host, final String user, final File priKeyFile) throws IOException {
        this.user = user;
        this.host = host;
        (this.conn = new Connection(host)).connect();
        final boolean auth = this.conn.authenticateWithPublicKey(user, priKeyFile, "");
        if (!auth) {
            throw new IOException("The private key (" + priKeyFile + ") is invalided!");
        }
        this.init();
    }
    
    private void init() {
        this.scpClient = new SCPClient(this.conn);
        this.shell = new SSHShell(this.conn, LogUtil.getInfoStream(SSHClient.LOG), LogUtil.getErrorStream(SSHClient.LOG));
    }
    
    public void scp(final String host, final String userName, final String dest, final String... src) throws IOException {
        this.shell.scp(host, userName, dest, src);
    }
    
    public void copyToLocal(final String remoteFile, final OutputStream out) throws IOException {
        this.scpClient.get(remoteFile, out);
    }
    
    public void copyToLocal(final String remoteFile, final File destDir) throws IOException {
        this.copyToLocal(remoteFile, destDir.toString());
    }
    
    public void copyToLocal(final String remoteFile, final String destDir) throws IOException {
        this.scpClient.get(remoteFile, destDir);
    }
    
    public void copyToLocal(final String[] remoteFiles, final String destDir) throws IOException {
        this.scpClient.get(remoteFiles, destDir);
    }
    
    public void copyToRemote(final String[] localFiles, final String destDir) throws IOException {
        this.scpClient.put(localFiles, destDir);
    }
    
    public void copyToRemote(final String localFile, final String destDir) throws IOException {
        this.scpClient.put(localFile, destDir);
    }
    
    public InputStream exeCommand(final boolean wait, final String command, final String... args) throws IOException {
        return this.shell.exeCommand(wait, command, args);
    }
    
    public void mkdir(final boolean wait, final String dir) throws IOException {
        this.shell.mkdir(wait, dir);
    }
    
    public void copyToRemote(final File localFile, final String destDir) throws IOException {
        if (localFile.isDirectory()) {
            final String targetDir = destDir + "/" + localFile.getName();
            SSHClient.LOG.info("Will  make remote dir " + targetDir);
            this.shell.mkdir(true, targetDir);
            final File[] arr$;
            final File[] children = arr$ = localFile.listFiles();
            for (final File child : arr$) {
                this.copyToRemote(child, targetDir);
            }
        }
        else {
            SSHClient.LOG.info("Will scp src = " + localFile + " target = " + destDir);
            this.scpClient.put(localFile.getAbsolutePath(), destDir);
        }
    }
    
    public boolean exist(final String destFile) throws IOException {
        final InputStream in = this.shell.exeCommand(true, "if [ -d '" + destFile + "' ];then echo 'true';else echo 'false'; fi", "");
        final BufferedReader br = StreamUtil.getBufferedReader(in);
        final String re = br.readLine();
        br.close();
        return StringUtil.toBoolean(re);
    }
    
    @Override
    public void close() {
        if (this.conn != null) {
            this.conn.close();
        }
    }
    
    public Connection getConn() {
        return this.conn;
    }
    
    public String getUser() {
        return this.user;
    }
    
    public String getHost() {
        return this.host;
    }
    
    public Shell getShell() {
        return this.shell;
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)SSHClient.class);
    }
}
