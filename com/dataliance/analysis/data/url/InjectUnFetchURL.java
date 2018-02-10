package com.dataliance.analysis.data.url;

import com.dataliance.service.util.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import com.dataliance.hadoop.hbase.*;
import com.dataliance.analysis.data.url.mapreduce.*;
import org.apache.hadoop.mapreduce.*;

public class InjectUnFetchURL extends HbaseTool
{
    public static void main(final String[] args) throws Exception {
        final Configuration conf = ConfigUtils.getConfig();
        ToolRunner.run(conf, (Tool)new InjectUnFetchURL(), args);
    }
    
    @Override
    protected void doAction(final Path[] in) throws Exception {
        this.addInputPath(in);
        this.setMapperClass(InjectURLMapper.class);
        this.runJob(true);
    }
}
