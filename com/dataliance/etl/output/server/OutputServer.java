package com.dataliance.etl.output.server;

import com.dataliance.etl.inject.server.*;
import com.dataliance.etl.output.*;
import com.dataliance.etl.output.rpc.*;
import com.dataliance.etl.output.vo.*;
import com.dataliance.util.*;

import org.apache.hadoop.conf.*;
import java.net.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.io.*;

import java.io.*;
import org.apache.hadoop.fs.*;
import com.dataliance.service.util.*;

import org.slf4j.*;

public class OutputServer extends AbstractServer implements Outputer
{
    private static final Logger LOG;
    public static final String STOP_FILE_OUTPUT = ".stop_output";
    public static final String SPLIT = "-";
    public static final int SIZE = 5;
    private OutputClient client;
    private String host;
    private String programID;
    private FileSystem fs;
    private File stopFile;
    
    public OutputServer(final Configuration conf, final String master, final int port, final String programID) throws IOException {
        super(conf);
        this.host = InetAddress.getLocalHost().getHostName();
        this.programID = programID;
        this.fs = FileSystem.get(conf);
        this.client = (OutputClient)RPC.getProxy((Class)OutputClient.class, 1L, new InetSocketAddress(master, port), conf);
    }
    
    @Override
    public void doOutput() throws IOException {
        final Text h = new Text(this.host);
        OutputVO outputVO = this.client.regist(h);
        this.stopFile = new File(outputVO.getProgramPath(), ".stop_output/" + this.programID);
        while (!outputVO.isNull()) {
            OutputServer.LOG.info(outputVO.toString());
            if (this.parse(outputVO)) {
                this.client.finish(h, outputVO);
            }
            else {
                this.client.error(h, outputVO);
            }
            if (this.stopFile.exists()) {
                break;
            }
            outputVO = this.client.next(h);
        }
        if (this.stopFile.exists()) {
            this.stopFile.deleteOnExit();
        }
        OutputServer.LOG.info("Will stop output server host = " + this.host);
    }
    
    private boolean parse(final OutputVO outputVO) {
        final Path src = new Path(outputVO.getSrc());
        File outFile = null;
        final String name = src.getName();
        if (outputVO.isSplit()) {
            outFile = new File(outputVO.getDestDir(), name + "-" + NumFormat.suffixByZero(outputVO.getStart(), 5));
        }
        else {
            outFile = new File(outputVO.getDestDir(), name);
        }
        if (outFile.exists()) {
            final String msg = "Output path '" + outFile + "' is exists ! Will not output src path = " + outputVO.getSrc();
            OutputServer.LOG.warn(msg);
            this.client.report(new Text(this.host), new Text(msg));
            return false;
        }
        try {
            final FSDataInputStream in = this.fs.open(src);
            if (!outFile.getParentFile().exists()) {
                outFile.getParentFile().mkdirs();
            }
            if (outputVO.isSplit()) {
                in.seek(outputVO.getStart());
                StreamUtil.output((InputStream)in, new FileOutputStream(outFile), outputVO.getLimit());
            }
            else {
                StreamUtil.output((InputStream)in, new FileOutputStream(outFile));
            }
            return true;
        }
        catch (Exception e) {
            OutputServer.LOG.error(e.getMessage(), (Throwable)e);
            return false;
        }
    }
    
    public static void main(final String[] args) throws IOException {
        final String usAge = "Usage : OutputServer <master> <port> <programID>";
        if (args.length < 3) {
            System.err.println(usAge);
            return;
        }
        final Configuration conf = ConfigUtils.getConfig();
        final String master = args[0];
        final int port = StringUtil.toInt(args[1]);
        final String programID = args[2];
        final OutputServer os = new OutputServer(conf, master, port, programID);
        os.doOutput();
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)Outputer.class);
    }
}
