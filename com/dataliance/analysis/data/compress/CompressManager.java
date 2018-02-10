package com.dataliance.analysis.data.compress;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import com.cms.framework.model.bigdata.*;
import com.dataliance.etl.inject.local.*;
import com.dataliance.util.*;

import org.apache.hadoop.fs.*;

import java.io.*;
import org.slf4j.*;
import java.util.*;

public class CompressManager extends Configured
{
    private static final Logger LOG;
    private static Map<Long, Job> jobs;
    private static Map<Long, InputStream> logs;
    private String command;
    private FileSystem fs;
    
    public CompressManager(final Configuration conf) throws IOException {
        super(conf);
        this.command = "/opt/brainbook/bigdata-core/run/compress";
        this.command = conf.get("com.DA.compress.jarname", "/opt/brainbook/bigdata-core/run/compress");
        this.fs = FileSystem.get(conf);
    }
    
    public InputStream submitJob(final Compression compress) throws Throwable {
        final Executor executor = new Executor("sh");
        executor.addArgument(this.command);
        executor.addArgument(compress.getInput());
        executor.addArgument(compress.getOutput());
        executor.addArgument(compress.getFormat());
        executor.addArgument(Long.toString(compress.getId()));
        executor.execute();
        final Thread errThread = new ProcessConsole(executor.getErrorStream(), "ERR", LogUtil.getErrorStream(CompressManager.LOG));
        errThread.start();
        final Thread infoThread = new ProcessConsole(executor.getInputStream(), "INFO", LogUtil.getInfoStream(CompressManager.LOG));
        infoThread.start();
        return null;
    }
    
    public boolean outExists(final String out) throws IOException {
        return this.fs.exists(new Path(out));
    }
    
    public InputStream getLog(final long id) throws IOException {
        final InputStream sb = CompressManager.logs.get(id);
        return sb;
    }
    
    public boolean isFinish(final long id) throws IOException {
        final Job job = CompressManager.jobs.get(id);
        return job == null || job.isComplete();
    }
    
    public static void main(final String[] args) throws Throwable {
        final String usage = "<in> <out> <compress> <id>";
        if (args.length < 4) {
            System.err.println(usage);
            return;
        }
        final Configuration conf = DAConfigUtil.create();
        final CompressManager manager = new CompressManager(conf);
        final Compression compression = new Compression();
        compression.setId(0L);
        compression.setInput(args[0]);
        compression.setOutput(args[1]);
        compression.setFormat(args[2]);
        final InputStream in = manager.submitJob(compression);
        final BufferedReader br = StreamUtil.getBufferedReader(in);
        for (String line = br.readLine(); line != null; line = br.readLine()) {
            System.out.println(line);
        }
    }
    
    static {
        LOG = LoggerFactory.getLogger((Class)CompressManager.class);
        CompressManager.jobs = new HashMap<Long, Job>();
        CompressManager.logs = new HashMap<Long, InputStream>();
    }
}
