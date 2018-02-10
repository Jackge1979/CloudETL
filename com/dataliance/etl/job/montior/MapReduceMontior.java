package com.dataliance.etl.job.montior;

import java.io.*;

import com.dataliance.etl.job.vo.*;

public interface MapReduceMontior extends Montior
{
    MapRedeceJob getJobStatus() throws IOException;
}
