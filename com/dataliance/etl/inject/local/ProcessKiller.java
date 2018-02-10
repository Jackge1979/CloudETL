package com.dataliance.etl.inject.local;

class ProcessKiller extends Thread
{
    private Process process;
    
    public ProcessKiller(final Process process) {
        this.process = process;
    }
    
    @Override
    public void run() {
        this.process.destroy();
    }
}
