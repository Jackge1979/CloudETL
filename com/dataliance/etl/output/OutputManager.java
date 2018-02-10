package com.dataliance.etl.output;

import java.io.*;

import com.dataliance.etl.io.*;

public interface OutputManager extends IOManager
{
    void deployOutputServer() throws IOException;
    
    void startOutputServer() throws IOException;
}
