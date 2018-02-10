package com.dataliance.etl.output.impl;

import org.apache.hadoop.conf.*;

import java.net.*;

import org.apache.hadoop.fs.*;
import java.io.*;

import com.dataliance.etl.inject.etl.vo.*;
import com.dataliance.etl.inject.etl.vo.http.*;
import com.dataliance.etl.inject.job.vo.*;
import com.dataliance.etl.inject.rpc.impl.*;
import com.dataliance.etl.io.*;
import com.dataliance.etl.job.montior.impl.*;
import com.dataliance.etl.job.vo.*;
import com.dataliance.etl.output.*;
import com.dataliance.etl.output.rpc.*;
import com.dataliance.etl.output.vo.*;
import com.dataliance.etl.workflow.process.*;
import com.dataliance.hadoop.util.*;
import com.dataliance.util.*;
import com.dataliance.service.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.*;
import java.util.*;

public class OutputManagerImpl extends AbstractManager implements OutputClient, OutputManager
{
    public static final String CONF_OUTPUT_SLAVES = "com.DA.server.output.slaves";
    public static final String CONF_OUTPUT_DESTPATH = "com.DA.ouput.program.destpath";
    public static final String CONF_OUTPUT_SERVER_COMMAND = "com.DA.server.output.command";
    public static final String OUTPUT_SLAVES = "output_slaves";
    private static final String OUTPUT_SERVER_COMMAND = "run/dataoutput output ";
    private static final Random ran;
    private static final List<OutputVO> waitOutput;
    private static final Map<Text, OutputVO> runningOutput;
    private static final Map<Text, OutputVO> finishOutput;
    private static final String STOP_FILE_OUTPUT = ".stop_output";
    private List<DirSource> dirSources;
    private List<Host> outputHosts;
    private DataSource dataSource;
    private String outputSlaves;
    private String destOutputPath;
    private String outputServerCom;
    private FileSystem fs;
    private float total;
    
    public OutputManagerImpl(final Configuration conf, final String programId, final String taskName, final int index, final int taskTotal) throws IOException {
        super(conf, programId, "OutputJob-" + programId, taskName, index, taskTotal);
        this.dirSources = new ArrayList<DirSource>();
        this.outputHosts = new ArrayList<Host>();
        this.dataSource = new HdfsDataSource();
        this.outputSlaves = conf.get("com.DA.server.output.slaves", "output_slaves");
        this.destOutputPath = conf.get("com.DA.ouput.program.destpath", "/opt/brainbook/bigdata-core");
        this.outputServerCom = conf.get("com.DA.server.output.command", "run/dataoutput output ");
        this.fs = FileSystem.get(conf);
    }
    
    @Override
    public void deployOutputServer() throws IOException {
        OutputManagerImpl.LOG.info("Will deploy Output server....");
        this.deployProgram(this.outputHosts, this.destOutputPath);
        OutputManagerImpl.LOG.info("Deploy Output server finish");
    }
    
    @Override
    public void startOutputServer() throws IOException {
        OutputManagerImpl.LOG.info("Will start output server....");
        final String command = this.destOutputPath + "/" + this.outputServerCom;
        final String logFile = this.logDir + "/" + "output_" + this.programId + ".log";
        this.startServer(this.outputHosts, "sh", command, this.host, Integer.toString(this.port), this.programId, ">>", logFile, "2>&1 &");
        OutputManagerImpl.LOG.info("Start output server finish");
    }
    
    public void init(final String inDir, final String outDir) throws IOException {
        final URL url = this.getConf().getResource(this.outputSlaves);
        if (url == null) {
            throw new IOException("The outputSlaves = " + this.outputSlaves + " does not exist");
        }
        final String[] inDirs = inDir.split(this.dirSplit);
        final String[] outDirs = outDir.split(this.dirSplit);
        this.initData(inDirs, outDirs);
        this.initOutput(url);
    }
    
    private void initData(final String[] inDirs, final String[] outDirs) throws IOException {
        for (int i = 0; i < inDirs.length; ++i) {
            final DirSource ds = new DirSource();
            ds.setSrc(inDirs[i]);
            if (outDirs.length == 1) {
                OutputManagerImpl.LOG.info("In dir = " + inDirs[i] + " outDir = " + outDirs[0]);
                ds.setDest(outDirs[0]);
            }
            else {
                OutputManagerImpl.LOG.info("In dir = " + inDirs[i] + " outDir = " + outDirs[i]);
                ds.setDest(outDirs[i]);
            }
            this.dirSources.add(ds);
        }
        this.parseDataSource();
    }
    
    private DataSource parseDataSource() throws IOException {
        OutputManagerImpl.LOG.info("Will parse data...");
        for (final DirSource dirSource : this.dirSources) {
            final Path src = new Path(dirSource.getSrc());
            final FileStatus fileStatus = this.fs.getFileStatus(src);
            if (fileStatus.isDir()) {
                this.parseData(src, dirSource.getDest());
            }
            else {
                final DataImpl data = new DataImpl();
                data.setDir(fileStatus.isDir());
                data.setSrc(dirSource.getSrc());
                data.setDestDir(dirSource.getDest());
                data.setLength(fileStatus.getLen());
                data.setStatus((byte)0);
                this.dataSource.addData(data);
            }
        }
        return this.dataSource;
    }
    
    private void parseData(final Path dir, final String destDir) throws IOException {
        final FileStatus[] arr$;
        final FileStatus[] paths = arr$ = this.fs.listStatus(dir);
        for (final FileStatus path : arr$) {
            if (path.isDir()) {
                final File file = new File(destDir, path.getPath().getName());
                this.parseData(path.getPath(), file.toString());
            }
            else {
                final DataImpl data = new DataImpl();
                data.setDir(false);
                data.setSrc(path.getPath().toString());
                data.setDestDir(destDir);
                data.setLength(path.getLen());
                data.setStatus((byte)0);
                this.dataSource.addData(data);
            }
        }
    }
    
    private void listData() throws IOException {
        OutputManagerImpl.LOG.info("Will list HttpDataSouce = " + this.dataSource);
        final Map<String, Data> datas = this.dataSource.getDatas();
        if (this.split && this.limit > 0L) {
            for (final Data data : datas.values()) {
                if (data.length() > this.limit) {
                    for (long i = 0L; i < data.length(); i += this.limit) {
                        OutputManagerImpl.waitOutput.add(this.parse(data, this.limit, i, true));
                    }
                }
                else {
                    OutputManagerImpl.waitOutput.add(this.parse(data, 0L, 0L, false));
                }
            }
        }
        else {
            for (final Data data : datas.values()) {
                OutputManagerImpl.waitOutput.add(this.parse(data, 0L, 0L, false));
            }
        }
        this.startOutputServer();
        OutputManagerImpl.LOG.info("List host = " + this.host + " HttpDataSouce = " + this.dataSource + " finish");
    }
    
    private OutputVO parse(final Data data, final long limit, final long start, final boolean sp) {
        final OutputVO out = new OutputVO();
        out.setNull(false);
        out.setLimit(limit);
        out.setSrc(data.getSrc());
        out.setStart(start);
        data.setStatus((byte)0);
        out.setLength(data.length());
        out.setDestDir(data.getDestDir());
        out.setSplit(sp);
        out.setProgramPath(this.destOutputPath);
        return out;
    }
    
    private void initOutput(final URL url) throws IOException {
        OutputManagerImpl.LOG.info("Will init output url  = " + url);
        final List<Host> hosts = this.listHost(url);
        OutputManagerImpl.LOG.info("Output hosts  = " + hosts);
        this.outputHosts.addAll(hosts);
    }
    
    public void commit() throws IOException {
        this.listData();
    }
    
    public static void main(final String[] args) throws IOException {
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
        final OutputManagerImpl outManager = new OutputManagerImpl(conf, programId, taskName, index, total);
        final String inDir = jobInfo.get(ETLConstants.INPUT_DATA.ID_from.toString());
        final String outDir = jobInfo.get(ETLConstants.OUTPUT_DATA.OD_target.toString());
        outManager.init(inDir, outDir);
        outManager.commit();
    }
    
    private void setStatus(final Text host, final OutputVO outputVO, final byte status) {
        final Map<String, Data> datas = this.dataSource.getDatas();
        final Data data = datas.get(outputVO.getSrc());
        data.setStatus(status);
        data.addParseHost(host.toString());
    }
    
    @Override
    public OutputVO regist(final Text host) throws IOException {
        OutputManagerImpl.LOG.info("Will regist output server host = " + host);
        final OutputVO ovo = this.getNextVO();
        if (!ovo.isNull()) {
            this.setStatus(host, ovo, (byte)1);
            OutputManagerImpl.runningOutput.put(host, ovo);
        }
        return ovo;
    }
    
    @Override
    public void finish(final Text host, final OutputVO outputVO) throws IOException {
        OutputManagerImpl.runningOutput.remove(host);
        this.setStatus(host, outputVO, (byte)2);
        OutputManagerImpl.finishOutput.put(host, outputVO);
        if (OutputManagerImpl.waitOutput.isEmpty() && OutputManagerImpl.runningOutput.isEmpty()) {
            this.close();
        }
    }
    
    @Override
    public void error(final Text host, final OutputVO outputVO) throws IOException {
        OutputManagerImpl.runningOutput.remove(host);
        this.setStatus(host, outputVO, (byte)(-1));
        OutputManagerImpl.finishOutput.put(host, outputVO);
        if (OutputManagerImpl.waitOutput.isEmpty() && OutputManagerImpl.runningOutput.isEmpty()) {
            this.close();
        }
    }
    
    @Override
    public OutputVO next(final Text host) throws IOException {
        final OutputVO ovo = this.getNextVO();
        if (!ovo.isNull()) {
            this.setStatus(host, ovo, (byte)1);
            OutputManagerImpl.runningOutput.put(host, ovo);
        }
        return ovo;
    }
    
    private OutputVO getNextVO() {
        synchronized (OutputManagerImpl.waitOutput) {
            if (!OutputManagerImpl.waitOutput.isEmpty()) {
                return OutputManagerImpl.waitOutput.remove(OutputManagerImpl.ran.nextInt(OutputManagerImpl.waitOutput.size()));
            }
            return OutputVO.getNull();
        }
    }
    
    public void close() throws IOException {
        OutputManagerImpl.LOG.info("Has no data for output");
        OutputManagerImpl.LOG.info("Will stop output servers");
        this.stopOutputervers();
        OutputManagerImpl.LOG.info("Will stop data servers");
        this.saveStatus();
        OutputManagerImpl.LOG.info("After 5 sec system will exit!");
        new StopServer().start();
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
    
    private void stopOutputervers() throws IOException {
        final String path = this.destOutputPath + "/" + ".stop_output" + "/" + this.programId;
        this.stopServers(this.outputHosts, "output", path);
    }
    
    public com.dataliance.etl.inject.job.vo.Host getManagerHost() {
        final com.dataliance.etl.inject.job.vo.Host host = new com.dataliance.etl.inject.job.vo.Host();
        host.setHost(this.host);
        host.setPort(this.port);
        host.setHostType((byte)(-1));
        return host;
    }
    
    public HostList getAllDataHosts() {
        final HostList hostList = new HostList();
        final List<com.dataliance.etl.inject.job.vo.Host> hosts = new ArrayList<com.dataliance.etl.inject.job.vo.Host>();
        final com.dataliance.etl.inject.job.vo.Host h = new com.dataliance.etl.inject.job.vo.Host();
        h.setHost(this.dataSource.getHost());
        h.setPort(this.dataSource.getPort());
        h.setHostType((byte)0);
        final DataSource hData = this.dataSource;
        for (final Data dir : hData.getDatas().values()) {
            final com.dataliance.etl.inject.job.vo.Data data = new com.dataliance.etl.inject.job.vo.Data();
            data.setDataHost(h.getHost());
            data.setPath(dir.getSrc());
            data.setDataSize(dir.length());
            data.setStatus(dir.getStatus());
            for (final String ih : data.getImportHosts()) {
                data.addImportHost(ih);
            }
            h.addData(data);
        }
        hosts.add(h);
        hostList.setHosts(hosts);
        return hostList;
    }
    
    public HostList getAllImportHosts() {
        final HostList hostlist = new HostList();
        final List<com.dataliance.etl.inject.job.vo.Host> hosts = new ArrayList<com.dataliance.etl.inject.job.vo.Host>();
        for (final Host host : this.outputHosts) {
            final com.dataliance.etl.inject.job.vo.Host h = new com.dataliance.etl.inject.job.vo.Host();
            h.setHost(host.getHost());
            h.setHostType((byte)2);
            hosts.add(h);
        }
        hostlist.setHosts(hosts);
        return hostlist;
    }
    
    private DataList getDatasByStatus(final byte status) {
        final DataList dataList = new DataList();
        final List<com.dataliance.etl.inject.job.vo.Data> datas = new ArrayList<com.dataliance.etl.inject.job.vo.Data>();
        for (final Data data : this.dataSource.getDatas().values()) {
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
        dataList.setDatas(datas);
        return dataList;
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
    
    static {
        ran = new Random();
        waitOutput = Collections.synchronizedList(new LinkedList<OutputVO>());
        runningOutput = Collections.synchronizedMap(new HashMap<Text, OutputVO>());
        finishOutput = Collections.synchronizedMap(new HashMap<Text, OutputVO>());
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
            OutputManagerImpl.this.server.stop();
        }
    }
}
