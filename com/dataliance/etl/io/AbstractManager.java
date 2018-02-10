package com.dataliance.etl.io;

import org.apache.hadoop.conf.*;

import java.net.*;

import com.dataliance.etl.inject.etl.vo.*;
import com.dataliance.etl.inject.local.*;
import com.dataliance.etl.inject.rpc.*;
import com.dataliance.etl.inject.ssh.*;
import com.dataliance.etl.job.option.*;
import com.dataliance.etl.job.vo.*;
import com.dataliance.util.*;

import java.io.*;
import org.apache.hadoop.io.*;
import java.util.*;

import org.apache.hadoop.ipc.*;
import org.slf4j.*;

public abstract class AbstractManager extends Configured implements IOManager, JobClient
{
    public static final Logger LOG;
    public static final String CONF_HOST = "com.DA.import.host";
    public static final String CONF_PORT = "com.DA.import.port";
    public static final String CONF_SPLIT = "com.DA.import.split";
    public static final String CONF_LIMIT = "com.DA.import.limit";
    public static final String CONF_PROGRAM_PATH = "com.DA.import.program.path";
    public static final String CONF_IN_OUT_DIR_SPLIT = "com.DA.import.dir.split";
    public static final String IN_OUT_DIR_SPLIT = ",";
    public static final String PROGRAM_ID_FILE = ".progarm_id";
    public static final String PROGRAM_FINISH = ".finish";
    public static final String LOG_DIR_NAME = "logs";
    public static final long NO_SPLIT_LIMIT = 0L;
    public static final long NO_SPLIT_START = 0L;
    protected int port;
    protected boolean split;
    protected long limit;
    protected String host;
    protected String programPath;
    protected String logDir;
    protected String dirSplit;
    protected String programId;
    protected String jobName;
    protected File priKeyFile;
    protected RPC.Server server;
    protected JobOption jobOption;
    protected String jobService;
    protected String taskName;
    protected int index;
    protected int taskTotal;
    
    public AbstractManager(final Configuration conf, final String programId, final String jobName, final String taskName, final int index, final int taskTotal) throws IOException {
        super(conf);
        final String host = InetAddress.getLocalHost().getHostName();
        this.jobService = this.getConf().get("com.DA.job.service", "http://" + host + ":8080/updateTaskStatus");
        this.port = conf.getInt("com.DA.import.port", 0);
        this.split = conf.getBoolean("com.DA.import.split", false);
        this.limit = conf.getLong("com.DA.import.limit", 67108864L);
        this.host = conf.get("com.DA.import.host", host);
        this.programPath = conf.get("com.DA.import.program.path", "/opt/brainbook/bigdata-core");
        this.logDir = this.programPath + "/" + "logs";
        this.dirSplit = conf.get("com.DA.import.dir.split", ",");
        this.priKeyFile = StreamUtil.getPriKeyFile();
        this.server = RPC.getServer((Object)this, host, this.port, conf);
        this.port = this.server.getListenerAddress().getPort();
        this.jobOption = new JobOption(conf);
        this.programId = programId;
        this.jobName = jobName;
        this.taskName = taskName;
        this.index = index;
        this.taskTotal = taskTotal;
        this.start();
        this.startMonitor();
    }
    
    private void startMonitor() throws IOException {
        final JobInfo jobInfo = new JobInfo();
        jobInfo.setHost(this.host);
        jobInfo.setJobName(this.jobName);
        jobInfo.setPort(this.port);
        jobInfo.setProgramID(this.programId);
        jobInfo.setStatus(JobInfo.STATUS.RUNNING);
        jobInfo.setType(JobInfo.JOB_TYPE.PROGRAM);
        jobInfo.setTaskName(this.taskName);
        jobInfo.setIndex(this.index);
        jobInfo.setStartTime(System.currentTimeMillis());
        this.jobOption.insert(jobInfo);
    }
    
    protected void doPush(final JobInfo.STATUS status) throws IOException {
        URL url = null;
        if (status == JobInfo.STATUS.FINISH) {
            if (this.index == this.taskTotal) {
                url = new URL(this.jobService + "?id=" + this.programId + "&taskName=" + this.taskName + "&status=" + JobInfo.STATUS.FINISH.ordinal());
            }
            else {
                url = new URL(this.jobService + "?id=" + this.programId + "&taskName=" + this.taskName);
            }
        }
        else {
            url = new URL(this.jobService + "?id=" + this.programId + "&taskName=" + this.taskName + "&status=" + status.ordinal());
        }
        final BufferedReader br = StreamUtil.getBufferedReader(url.openStream());
        final String line = br.readLine();
        if (line != null && line.equals("0")) {
            System.out.println("SUCCESS");
        }
        else {
            System.out.println("ERROR");
        }
        br.close();
    }
    
    public void start() throws IOException {
        final ServerThread st = new ServerThread(this.server);
        st.start();
    }
    
    protected void startServer(final List<Host> hosts, final String command, final String... args) throws IOException {
        for (final Host host : hosts) {
            final SSHClient sshClient = this.getSSHClient(host);
            AbstractManager.LOG.info("Will exe command = " + command + " args = " + Arrays.toString(args));
            if (!sshClient.exist(this.logDir)) {
                sshClient.mkdir(true, this.logDir);
            }
            final InputStream in = sshClient.exeCommand(false, command, args);
            new ProcessConsole(in, "OUT", LogUtil.getInfoStream(AbstractManager.LOG)).start();
            sshClient.close();
        }
    }
    
    private SSHClient getSSHClient(final Host host) throws IOException {
        SSHClient sshClient = null;
        if (host.isUsePassword()) {
            sshClient = new SSHClient(host.getHost(), host.getUser(), host.getPassword());
        }
        else {
            sshClient = new SSHClient(host.getHost(), host.getUser(), this.priKeyFile);
        }
        return sshClient;
    }
    
    protected void deployProgram(final List<Host> hosts, final String destPath) throws IOException {
        final File programFile = new File(this.programPath);
        for (final Host host : hosts) {
            final SSHClient sshClient = this.getSSHClient(host);
            if (!sshClient.exist(destPath)) {
                sshClient.mkdir(true, destPath);
                AbstractManager.LOG.info("Will copy host = " + host + " programFile = " + programFile + " destPath = " + destPath);
                if (programFile.isFile()) {
                    sshClient.copyToRemote(programFile, destPath);
                }
                else {
                    final File[] arr$;
                    final File[] children = arr$ = programFile.listFiles();
                    for (final File file : arr$) {
                        sshClient.copyToRemote(file, destPath);
                    }
                }
                AbstractManager.LOG.info("Copy finish!");
            }
            else {
                AbstractManager.LOG.info("host = " + host + " destPath = " + destPath + " exist!");
            }
            sshClient.close();
        }
    }
    
    public void report(final Text host, final Text msg) {
        AbstractManager.LOG.info("host = " + host + " msg = " + msg);
    }
    
    protected void stopServers(final List<Host> hosts, final String serverType, final String stopPath) throws IOException {
        for (final Host host : hosts) {
            final SSHClient sshClient = this.getSSHClient(host);
            AbstractManager.LOG.info("Will stop " + serverType + " server hostname = " + host);
            sshClient.mkdir(true, stopPath + "/" + this.programId);
            sshClient.close();
        }
    }
    
    protected List<Host> listHost(final URL slaves) throws IOException {
        final List<Host> hosts = new ArrayList<Host>();
        final BufferedReader br = StreamUtil.getBufferedReader(slaves.openStream());
        for (String line = br.readLine(); line != null; line = br.readLine()) {
            if (!this.skipComment(line)) {
                final Host host = new Host();
                final String[] vs = line.split("\\s+");
                final String hostname = vs[0];
                String user = StreamUtil.getUser();
                String password = null;
                if (vs.length >= 2) {
                    user = vs[1];
                    if (vs.length == 3) {
                        password = vs[2];
                    }
                }
                host.setHost(hostname);
                host.setUser(user);
                host.setPassword(password);
                host.setUsePassword(!StringUtil.isEmpty(host.getPassword()));
                hosts.add(host);
            }
        }
        return hosts;
    }
    
    protected boolean skipComment(final String line) {
        return StringUtil.isEmpty(line) || line.startsWith("#");
    }
    
    public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {
        return 1L;
    }
    
    public ProtocolSignature getProtocolSignature(final String arg0, final long arg1, final int arg2) throws IOException {
        return null;
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)IOManager.class);
    }
    
    class ServerThread extends Thread
    {
        private final RPC.Server server;
        
        public ServerThread(final RPC.Server server) {
            this.server = server;
        }
        
        @Override
        public void run() {
            try {
                this.server.start();
                this.server.join();
            }
            catch (Exception e) {
                AbstractManager.LOG.error(e.getMessage(), (Throwable)e);
            }
        }
    }
}
