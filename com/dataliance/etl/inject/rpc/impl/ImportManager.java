package com.dataliance.etl.inject.rpc.impl;
import org.apache.hadoop.conf.*;

import java.net.*;
import java.io.*;

import org.apache.hadoop.io.*;

import com.dataliance.etl.inject.etl.vo.*;
import com.dataliance.etl.inject.etl.vo.Host;
import com.dataliance.etl.inject.etl.vo.http.*;
import com.dataliance.etl.inject.job.vo.*;
import com.dataliance.etl.inject.local.*;
import com.dataliance.etl.inject.rpc.*;
import com.dataliance.etl.inject.ssh.*;
import com.dataliance.etl.job.montior.impl.*;
import com.dataliance.etl.job.option.*;
import com.dataliance.etl.job.vo.*;
import com.dataliance.etl.workflow.process.*;
import com.dataliance.hadoop.util.*;
import com.dataliance.util.*;
import com.dataliance.service.util.*;

import org.apache.hadoop.ipc.*;
import org.slf4j.*;
import java.util.*;
import java.util.logging.Logger;

public class ImportManager extends Configured implements Manager
{
    private static final Logger LOG;
    private static final Map<Text, ImportVO> runningImport;
    private static final Map<Text, ImportVO> finishImport;
    private static final Map<Text, DataSource> dataSourceMap;
    private static final Map<Text, FloatWritable> statusMap;
    private static final List<ImportVO> waitImport;
    public static final String CONF_HOST = "com.da.import.host";
    public static final String CONF_PORT = "com.da.import.port";
    public static final String CONF_SPLIT = "com.da.import.split";
    public static final String CONF_LIMIT = "com.da.import.limit";
    public static final String CONF_PROGRAM_PATH = "com.da.import.program.path";
    public static final String CONF_IMPORT_DESTPATH = "com.da.import.program.destpath";
    public static final String CONF_DATA_DESTPATH = "com.da.data.program.destpath";
    public static final String CONF_DATA_SERVER_COMMAND = "com.da.server.data.command";
    public static final String CONF_IMPORT_SERVER_COMMAND = "com.da.server.import.command";
    public static final String CONF_IMPORT_SLAVES = "com.da.server.import.slaves";
    public static final String CONF_DATA_SLAVES = "com.da.server.data.slaves";
    public static final String CONF_IN_OUT_DIR_SPLIT = "com.da.import.dir.split";
    private static final String DATA_SERVER_COMMAND = "run/dataimport data ";
    private static final String IMPORT_SERVER_COMMAND = "run/dataimport import ";
    public static final String IMPORT_SLAVES = "import_slaves";
    public static final String DATA_SLAVES = "data_slaves";
    public static final String IN_OUT_DIR_SPLIT = ",";
    public static final String PROGRAM_ID_FILE = ".progarm_id";
    public static final String PROGRAM_FINISH = ".finish";
    public static final String LOG_DIR_NAME = "logs";
    private static final Random ran;
    private static final long NO_SPLIT_LIMIT = 0L;
    private static final long NO_SPLIT_START = 0L;
    private List<Host> importHosts;
    private List<Host> dataHosts;
    private RPC.Server server;
    private JobOption jobOption;
    private String host;
    private int port;
    private boolean split;
    private long limit;
    private String programPath;
    private String logDir;
    private String destImportPath;
    private String destDataPath;
    private String dataServerCom;
    private String importServerCom;
    private String importSlaves;
    private String dataSlaves;
    private String dirSplit;
    private String programId;
    private File priKeyFile;
    private String taskName;
    private int index;
    private int taskTotal;
    private String jobService;
    private boolean useSet;
    private Map<String, String> params;
    private float total;
    
    public ImportManager(final Configuration conf, final String programId, final String taskName, final int index, final int taskTotal) throws IOException {
        super(conf);
        this.importHosts = new ArrayList<Host>();
        this.dataHosts = new ArrayList<Host>();
        this.useSet = false;
        this.params = new HashMap<String, String>();
        this.port = conf.getInt("com.DA.import.port", 0);
        this.split = conf.getBoolean("com.DA.import.split", false);
        this.limit = conf.getLong("com.DA.import.limit", 67108864L);
        this.host = conf.get("com.DA.import.host", InetAddress.getLocalHost().getHostName());
        this.programPath = conf.get("com.DA.import.program.path", "/opt/brainbook/bigdata-core");
        this.logDir = this.programPath + "/" + "logs";
        this.destImportPath = conf.get("com.DA.import.program.destpath", "/opt/brainbook/bigdata-core");
        this.destDataPath = conf.get("com.DA.data.program.destpath", "/opt/brainbook/bigdata-core");
        this.dataServerCom = conf.get("com.DA.server.data.command", "run/dataimport data ");
        this.importServerCom = conf.get("com.DA.server.import.command", "run/dataimport import ");
        this.importSlaves = conf.get("com.DA.server.import.slaves", "import_slaves");
        this.dataSlaves = conf.get("com.DA.server.data.slaves", "data_slaves");
        this.dirSplit = conf.get("com.DA.import.dir.split", ",");
        this.priKeyFile = StreamUtil.getPriKeyFile();
        this.server = RPC.getServer((Object)this, this.host, this.port, conf);
        this.port = this.server.getListenerAddress().getPort();
        this.jobOption = new JobOption(conf);
        this.programId = programId;
        this.taskName = taskName;
        this.index = index;
        this.taskTotal = taskTotal;
        this.jobService = this.getConf().get("com.DA.job.service", "http://" + InetAddress.getLocalHost().getHostName() + ":8080/updateTaskStatus");
        this.updateMonitor();
    }
    
    private void updateMonitor() throws IOException {
        final JobInfo jobInfo = new JobInfo();
        jobInfo.setHost(this.host);
        jobInfo.setJobName("importJob-" + this.programId);
        jobInfo.setPort(this.port);
        jobInfo.setIndex(this.index);
        jobInfo.setTaskName(this.taskName);
        jobInfo.setProgramID(this.programId);
        jobInfo.setStatus(JobInfo.STATUS.RUNNING);
        jobInfo.setType(JobInfo.JOB_TYPE.PROGRAM);
        this.jobOption.insert(jobInfo);
    }
    
    private void saveStatus() throws IOException {
        final JobInfo jobInfo = this.jobOption.get(this.programId);
        jobInfo.setEndTime(System.currentTimeMillis());
        jobInfo.setStatus(JobInfo.STATUS.FINISH);
        final ProFinishMonitor finish = new ProFinishMonitor();
        finish.setManagerHost(this.getManagerHost());
        finish.setDataHostList(this.getAllDataHosts());
        finish.setImportHostList(this.getAllImportHosts());
        finish.setDataList(this.getAllDatas());
        finish.setErrDataList(this.getAllErrorDatas());
        finish.setFinishDataList(this.getAllFinishDatas());
        finish.setRunDataList(this.getAllRunDatas());
        finish.setWaitDataList(this.getAllWaitDatas());
        finish.setComplete(this.getComplete());
        jobInfo.setJobData(WriteableUtil.toBytes((Writable)finish));
        this.jobOption.insert(jobInfo);
        this.doPush(JobInfo.STATUS.FINISH);
    }
    
    public void setParams(String service, final Map<String, String> params) throws IOException {
        this.useSet = true;
        if (!service.startsWith("/")) {
            service = "/" + service;
        }
        this.jobService = this.getConf().get("com.DA.job.service", "http://" + InetAddress.getLocalHost().getHostName() + ":8080" + service);
        this.params = params;
    }
    
    private void doPush(final JobInfo.STATUS status) throws IOException {
        URL url = null;
        if (this.useSet) {
            final StringBuffer sb = new StringBuffer();
            sb.append("status").append("=");
            if (status == JobInfo.STATUS.FINISH) {
                sb.append("2");
            }
            else {
                sb.append("3");
            }
            sb.append("&");
            if (this.params != null) {
                for (final Map.Entry<String, String> entry : this.params.entrySet()) {
                    sb.append(entry.getKey()).append("=").append(entry.getValue()).append("&");
                }
            }
            sb.setLength(sb.length() - 1);
            url = new URL(this.jobService + "?" + (Object)sb);
        }
        else if (status == JobInfo.STATUS.FINISH) {
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
        ImportManager.LOG.info("Will use url = " + url);
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
    
    public void close() throws IOException {
        ImportManager.LOG.info("Has no data for import");
        ImportManager.LOG.info("Will stop import servers");
        this.stopImportServers();
        ImportManager.LOG.info("Will stop data servers");
        this.stopDataServers();
        ImportManager.LOG.info("Will save finish status");
        this.saveStatus();
        ImportManager.LOG.info("After 5 sec system will exit!");
        new StopServer().start();
    }
    
    public void start() throws IOException {
        final ServerThread st = new ServerThread(this.server);
        st.start();
    }
    
    public void deployDataServer() throws IOException {
        ImportManager.LOG.info("Will deploy data server....");
        this.deploy(this.dataHosts, this.destDataPath);
        ImportManager.LOG.info("Deploy data server finish");
    }
    
    public void startDataServer() throws IOException {
        ImportManager.LOG.info("Will start data server....");
        final String command = this.destDataPath + "/" + this.dataServerCom;
        final String logFile = this.logDir + "/" + "data_" + this.programId + ".log";
        this.startServer(this.dataHosts, "sh", command, this.host, "" + this.port, this.programId, ">>", logFile, "2>&1 &");
        ImportManager.LOG.info("Start data server finish");
    }
    
    public void deployImportServer() throws IOException {
        ImportManager.LOG.info("Will deploy import server....");
        this.deploy(this.importHosts, this.destImportPath);
        ImportManager.LOG.info("Deploy import server finish");
    }
    
    public void startImportServer() throws IOException {
        ImportManager.LOG.info("Will start import server....");
        final String command = this.destDataPath + "/" + this.importServerCom;
        final String logPath = this.logDir + "/" + "import_" + this.programId + ".log";
        this.startServer(this.importHosts, "sh", command, this.host, Integer.toString(this.port), this.programId, ">>", logPath, "2>&1 &");
        ImportManager.LOG.info("Start import server finish");
    }
    
    private void startServer(final List<Host> hosts, final String command, final String... args) throws IOException {
        for (final Host host : hosts) {
            final SSHClient sshClient = this.getSSHClient(host);
            ImportManager.LOG.info("Will exe command = " + command + " args = " + Arrays.toString(args));
            if (!sshClient.exist(this.logDir)) {
                sshClient.mkdir(true, this.logDir);
            }
            final InputStream in = sshClient.exeCommand(false, command, args);
            new ProcessConsole(in, "OUT", LogUtil.getInfoStream(ImportManager.LOG)).start();
        }
    }
    
    private void deploy(final List<Host> hosts, final String destPath) throws IOException {
        final File programFile = new File(this.programPath);
        for (final Host host : hosts) {
            final SSHClient sshClient = this.getSSHClient(host);
            if (!sshClient.exist(destPath)) {
                sshClient.mkdir(true, destPath);
                ImportManager.LOG.info("Will copy host = " + host + " programFile = " + programFile + " destPath = " + destPath);
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
                ImportManager.LOG.info("Copy finish!");
            }
            else {
                ImportManager.LOG.info("host = " + host + " destPath = " + destPath + " exist!");
            }
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
    
    public ImportVO regist(final Text host) throws IOException {
        ImportManager.LOG.info("Will regist import server host = " + host);
        final ImportVO ivo = this.getNextVO();
        if (!ivo.isNull()) {
            this.setStatus(host, ivo, (byte)1);
            ImportManager.runningImport.put(host, ivo);
        }
        return ivo;
    }
    
    public ImportVO next(final Text host) throws IOException {
        final ImportVO ivo = this.getNextVO();
        if (!ivo.isNull()) {
            this.setStatus(host, ivo, (byte)1);
            ImportManager.runningImport.put(host, ivo);
        }
        return ivo;
    }
    
    public void finish(final Text host, final ImportVO importVO) throws IOException {
        ImportManager.runningImport.remove(host);
        this.setStatus(host, importVO, (byte)2);
        ImportManager.finishImport.put(host, importVO);
        if (ImportManager.waitImport.isEmpty() && ImportManager.runningImport.isEmpty()) {
            this.close();
        }
    }
    
    public void error(final Text host, final ImportVO importVO) throws IOException {
        ImportManager.runningImport.remove(host);
        this.setStatus(host, importVO, (byte)(-1));
        ImportManager.finishImport.put(host, importVO);
        if (ImportManager.waitImport.isEmpty() && ImportManager.runningImport.isEmpty()) {
            this.close();
        }
    }
    
    private void setStatus(final Text host, final ImportVO importVO, final byte status) {
        final DataSource ds = ImportManager.dataSourceMap.get(new Text(importVO.getHost()));
        final Map<String, Data> datas = ds.getDatas();
        final Data data = datas.get(importVO.getSource());
        data.setStatus(status);
        data.addParseHost(host.toString());
    }
    
    private ImportVO getNextVO() {
        synchronized (ImportManager.waitImport) {
            if (!ImportManager.waitImport.isEmpty()) {
                return ImportManager.waitImport.remove(ImportManager.ran.nextInt(ImportManager.waitImport.size()));
            }
            return ImportVO.getNull();
        }
    }
    
    public void list(final Text host, final HttpDataSource dataSource) throws IOException {
        ImportManager.LOG.info("Will list host = " + host + " HttpDataSouce = " + dataSource);
        ImportManager.dataSourceMap.put(host, dataSource);
        final Map<String, Data> datas = dataSource.getDatas();
        if (this.split && this.limit > 0L) {
            for (final Data data : datas.values()) {
                if (data.length() > this.limit) {
                    for (long i = 0L; i < data.length(); i += this.limit) {
                        ImportManager.waitImport.add(this.parse(dataSource, data, this.limit, i, true));
                    }
                }
                else {
                    ImportManager.waitImport.add(this.parse(dataSource, data, 0L, 0L, false));
                }
            }
        }
        else {
            for (final Data data : datas.values()) {
                ImportManager.waitImport.add(this.parse(dataSource, data, 0L, 0L, false));
            }
        }
        if (ImportManager.dataSourceMap.size() == this.dataHosts.size()) {
            this.startImportServer();
        }
        ImportManager.LOG.info("List host = " + host + " HttpDataSouce = " + dataSource.getHost() + " finish");
    }
    
    private ImportVO parse(final DataSource ds, final Data data, final long limit, final long start, final boolean split) {
        final ImportVO ivo = new ImportVO();
        ivo.setHost(ds.getHost());
        ivo.setPort(ds.getPort());
        ivo.setLimit(limit);
        ivo.setSource(data.getSrc());
        ivo.setStart(start);
        data.setStatus((byte)0);
        ivo.setLength(data.length());
        ivo.setDestDir(data.getDestDir());
        ivo.setSplit(split);
        ivo.setProgramPath(this.destImportPath);
        return ivo;
    }
    
    public void finish(final Text host) {
        ImportManager.dataSourceMap.remove(host);
    }
    
    public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {
        return 1L;
    }
    
    public void report(final Text host, final Text msg, final FloatWritable status) {
        ImportManager.statusMap.put(host, status);
        ImportManager.LOG.info("host = " + host + " msg = " + msg + " status = " + status);
    }
    
    public DataHost regist(final Text host, final IntWritable port) {
        ImportManager.LOG.info("Will regist host = " + host + " port = " + port);
        for (final Host dh : this.dataHosts) {
            if (dh.getHost().equals(host.toString())) {
                final DataHost dataHost = (DataHost)dh;
                dataHost.setPort(port.get());
                return dataHost;
            }
        }
        return null;
    }
    
    public void log(final Text host, final Text msg) {
        ImportManager.LOG.info("host = " + host + " msg = " + msg);
    }
    
    public void addDataHost(final Host dataHost) {
        final DataHost dh = (DataHost)dataHost;
        dh.setProgramPath(this.destDataPath);
        ImportManager.LOG.info("Will Add src = " + dh.getSources());
        this.dataHosts.add(dh);
    }
    
    public void addImportHost(final Host importHost) {
        this.importHosts.add(importHost);
    }
    
    public void commit() throws Exception {
        this.start();
        this.deployDataServer();
        this.deployImportServer();
        this.startDataServer();
    }
    
    public void init(final String inDir, final String outDir) throws IOException {
        URL dSlaves = this.getConf().getResource(this.dataSlaves);
        URL iSlaves = this.getConf().getResource(this.importSlaves);
        if (dSlaves == null && iSlaves == null) {
            throw new IOException("The dataSlaves = " + this.dataSlaves + " and importSlaves = " + this.importSlaves + " does not exist");
        }
        if (dSlaves == null) {
            dSlaves = iSlaves;
        }
        if (iSlaves == null) {
            iSlaves = dSlaves;
        }
        final String[] inDirs = inDir.split(this.dirSplit);
        final String[] outDirs = outDir.split(this.dirSplit);
        this.initData(dSlaves, inDirs, outDirs);
        this.initImport(iSlaves);
    }
    
    private void initData(final URL dSlaves, final String[] inDirs, final String[] outDirs) throws IOException {
        final BufferedReader br = StreamUtil.getBufferedReader(dSlaves.openStream());
        for (String line = br.readLine(); line != null; line = br.readLine()) {
            if (!this.skipComment(line)) {
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
                final DataHost dataHost = new DataHost();
                dataHost.setHost(hostname);
                dataHost.setPassword(password);
                dataHost.setProgramPath(this.destDataPath);
                if (StringUtil.isEmpty(password)) {
                    dataHost.setUsePassword(false);
                }
                else {
                    dataHost.setUsePassword(true);
                }
                dataHost.setUser(user);
                for (int i = 0; i < inDirs.length; ++i) {
                    final DirSource ds = new DirSource();
                    ds.setSrc(inDirs[i]);
                    if (outDirs.length == 1) {
                        ImportManager.LOG.info("In dir = " + inDirs[i] + " outDir = " + outDirs[0]);
                        ds.setDest(outDirs[0]);
                    }
                    else {
                        ImportManager.LOG.info("In dir = " + inDirs[i] + " outDir = " + outDirs[i]);
                        ds.setDest(outDirs[i]);
                    }
                    dataHost.addSource(ds);
                }
                this.addDataHost(dataHost);
            }
        }
        br.close();
    }
    
    private void initImport(final URL iSlaves) throws IOException {
        final BufferedReader br = StreamUtil.getBufferedReader(iSlaves.openStream());
        for (String line = br.readLine(); line != null; line = br.readLine()) {
            if (!this.skipComment(line)) {
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
                final ImportHost importHost = new ImportHost();
                importHost.setHost(hostname);
                importHost.setPassword(password);
                if (StringUtil.isEmpty(password)) {
                    importHost.setUsePassword(false);
                }
                else {
                    importHost.setUsePassword(true);
                }
                importHost.setUser(user);
                this.addImportHost(importHost);
            }
        }
        br.close();
    }
    
    private boolean skipComment(final String line) {
        return StringUtil.isEmpty(line) || line.startsWith("#");
    }
    
    private void stopDataServers() throws IOException {
        for (final Host host : this.dataHosts) {
            final SSHClient sshClient = this.getSSHClient(host);
            ImportManager.LOG.info("Will stop data server hostname = " + host);
            sshClient.mkdir(true, this.destDataPath + "/" + ".stop_data" + "/" + this.programId);
            sshClient.close();
        }
    }
    
    private void stopImportServers() throws IOException {
        for (final Host host : this.importHosts) {
            final SSHClient sshClient = this.getSSHClient(host);
            ImportManager.LOG.info("Will stop import server hostname = " + host);
            sshClient.mkdir(true, this.destImportPath + "/" + ".stop_import" + "/" + this.programId);
            sshClient.close();
        }
    }
    
    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("arg count : " + args.length);
            System.out.println(String.format("Usage : %s  {'name': 'value'}", ImportManager.class.getSimpleName()));
            System.exit(1);
        }
        Map<String, String> jobInfo = new HashMap<String, String>();
        try {
            jobInfo = ParameterParser.convertJson2Map(args[0]);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.print("arg: " + args[0]);
            System.out.println(String.format("Usage : %s  {'name': 'value'}", ImportManager.class.getSimpleName()));
            System.exit(1);
        }
        final String programId = jobInfo.get(ETLConstants.PROGRAM_ID.PD_id.toString());
        final String taskName = jobInfo.get(ETLConstants.JOB_MONTIOR.taskName.toString());
        final int index = Integer.parseInt(jobInfo.get(ETLConstants.JOB_MONTIOR.currentTaskIndex.toString()));
        final int total = Integer.parseInt(jobInfo.get(ETLConstants.JOB_MONTIOR.taskCount.toString()));
        if (StringUtil.isEmpty(programId)) {
            throw new IOException("The params why key = " + ETLConstants.PROGRAM_ID.PD_id.toString() + " is not exist");
        }
        final Configuration conf = ConfigUtils.getConfig();
        final ImportManager im = new ImportManager(conf, programId, taskName, index, total);
        final String inDir = jobInfo.get(ETLConstants.INPUT_DATA.ID_from.toString());
        final String outDir = jobInfo.get(ETLConstants.OUTPUT_DATA.OD_target.toString());
        im.init(inDir, outDir);
        im.commit();
    }
    
    public HostList getAllDataHosts() {
        final HostList hostList = new HostList();
        final List<com.dataliance.etl.inject.job.vo.Host> hosts = new ArrayList<com.dataliance.etl.inject.job.vo.Host>();
        for (final Host host : this.dataHosts) {
            final com.dataliance.etl.inject.job.vo.Host h = new com.dataliance.etl.inject.job.vo.Host();
            h.setHost(host.getHost());
            h.setPort(host.getPort());
            h.setHostType((byte)0);
            if (!ImportManager.dataSourceMap.isEmpty()) {
                final DataSource hData = ImportManager.dataSourceMap.get(new Text(h.getHost()));
                for (final Data dir : hData.getDatas().values()) {
                    final com.dataliance.etl.inject.job.vo.Data data = new com.dataliance.etl.inject.job.vo.Data();
                    data.setDataHost(host.getHost());
                    data.setPath(dir.getSrc());
                    data.setDataSize(dir.length());
                    data.setStatus(dir.getStatus());
                    for (final String ih : data.getImportHosts()) {
                        data.addImportHost(ih);
                    }
                    h.addData(data);
                }
            }
            hosts.add(h);
        }
        hostList.setHosts(hosts);
        return hostList;
    }
    
    public HostList getAllImportHosts() {
        final HostList hostlist = new HostList();
        final List<com.dataliance.etl.inject.job.vo.Host> hosts = new ArrayList<com.dataliance.etl.inject.job.vo.Host>();
        for (final Host host : this.importHosts) {
            final com.dataliance.etl.inject.job.vo.Host h = new com.dataliance.etl.inject.job.vo.Host();
            h.setHost(host.getHost());
            h.setHostType((byte)1);
            hosts.add(h);
        }
        hostlist.setHosts(hosts);
        return hostlist;
    }
    
    public DataList getAllDatas() {
        return this.getDatasByStatus((byte)(-2));
    }
    
    public DataList getAllFinishDatas() {
        return this.getDatasByStatus((byte)2);
    }
    
    public DataList getAllWaitDatas() {
        return this.getDatasByStatus((byte)0);
    }
    
    public DataList getAllRunDatas() {
        return this.getDatasByStatus((byte)1);
    }
    
    public DataList getAllErrorDatas() {
        return this.getDatasByStatus((byte)(-1));
    }
    
    private DataList getDatasByStatus(final byte status) {
        final DataList dataList = new DataList();
        final List<com.dataliance.etl.inject.job.vo.Data> datas = new ArrayList<com.dataliance.etl.inject.job.vo.Data>();
        for (final DataSource ds : ImportManager.dataSourceMap.values()) {
            for (final Data data : ds.getDatas().values()) {
                if (status != -2 && data.getStatus() != status) {
                    continue;
                }
                final com.dataliance.etl.inject.job.vo.Data d = new com.dataliance.etl.inject.job.vo.Data();
                d.setDataHost(data.getDataHost());
                d.setPath(data.getSrc());
                d.setDataSize(data.length());
                d.setStatus(data.getStatus());
                for (final String host : data.getParseHosts()) {
                    d.addImportHost(host);
                }
                datas.add(d);
            }
        }
        dataList.setDatas(datas);
        return dataList;
    }
    
    private float getAllSize() {
        final DataList datalist = this.getAllDatas();
        float all = 0.0f;
        for (final com.dataliance.etl.inject.job.vo.Data data : datalist.getDatas()) {
            all += data.getDataSize();
        }
        return all;
    }
    
    private float getFinishSize() {
        float finish = 0.0f;
        final DataList datalist = this.getAllFinishDatas();
        for (final com.dataliance.etl.inject.job.vo.Data data : datalist.getDatas()) {
            finish += data.getDataSize();
        }
        return finish;
    }
    
    private float getErrorSize() {
        float error = 0.0f;
        final DataList datalist = this.getAllErrorDatas();
        for (final com.dataliance.etl.inject.job.vo.Data data : datalist.getDatas()) {
            error += data.getDataSize();
        }
        return error;
    }
    
    public FloatWritable getComplete() {
        if (this.total == 0.0f) {
            this.total = this.getAllSize();
        }
        final float finish = this.getFinishSize();
        final float error = this.getErrorSize();
        if (this.total != 0.0f) {
            return new FloatWritable((finish + error) / this.total);
        }
        return new FloatWritable(0.0f);
    }
    
    public com.dataliance.etl.inject.job.vo.Host getManagerHost() {
        final com.dataliance.etl.inject.job.vo.Host host = new com.dataliance.etl.inject.job.vo.Host();
        host.setHost(this.host);
        host.setPort(this.port);
        host.setHostType((byte)(-1));
        return host;
    }
    
    public ProtocolSignature getProtocolSignature(final String protocol, final long version, final int model) throws IOException {
        return null;
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)ImportManager.class);
        runningImport = Collections.synchronizedMap(new HashMap<Text, ImportVO>());
        finishImport = Collections.synchronizedMap(new HashMap<Text, ImportVO>());
        dataSourceMap = Collections.synchronizedMap(new HashMap<Text, DataSource>());
        statusMap = Collections.synchronizedMap(new HashMap<Text, FloatWritable>());
        waitImport = Collections.synchronizedList(new LinkedList<ImportVO>());
        ran = new Random();
    }
    
    class StopServer extends Thread
    {
        @Override
        public void run() {
            try {
                Thread.sleep(5000L);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            ImportManager.this.server.stop();
        }
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
                ImportManager.LOG.error(e.getMessage(), (Throwable)e);
            }
        }
    }
}
