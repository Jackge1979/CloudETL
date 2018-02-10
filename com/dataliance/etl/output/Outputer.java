package com.dataliance.etl.output;

import java.io.*;

public interface Outputer
{
    void doOutput() throws IOException;
}
