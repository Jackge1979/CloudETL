package com.dataliance.service.util;

public class ThreadWorker extends Thread
{
    private MyThreadPool myThreadPool;
    
    public ThreadWorker(final MyThreadPool myThreadPool, final int id) {
        super(myThreadPool, "com.wintim.service" + id);
        this.myThreadPool = myThreadPool;
    }
    
    @Override
    public void run() {
        while (true) {
            if (this.myThreadPool.getWaitTaskCount() <= 0) {
                if (!this.myThreadPool.isActive()) {
                    break;
                }
            }
            try {
                final Runnable task = this.myThreadPool.receiveTask();
                if (task == null) {
                    continue;
                }
                task.run();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
