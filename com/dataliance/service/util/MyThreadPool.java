package com.dataliance.service.util;

import java.util.*;
import java.util.concurrent.atomic.*;
import org.apache.commons.logging.*;

public class MyThreadPool extends ThreadGroup
{
    private static final Log LOG;
    private static final boolean traceThread;
    private static int id;
    public static final String THREAD_GROUP_NAME = "com.wintim.service";
    public LinkedList waitQueue;
    private int coreThreadsCount;
    private ThreadWorker[] threadWorkers;
    private AtomicInteger waitedTaskCount;
    private AtomicBoolean isActive;
    
    public MyThreadPool(final int threadsCount) {
        super("com.wintim.service" + MyThreadPool.id);
        this.waitQueue = new LinkedList();
        this.waitedTaskCount = new AtomicInteger(0);
        this.isActive = new AtomicBoolean(false);
        if (threadsCount <= 0) {
            throw new IllegalArgumentException("The thread count must >= 1");
        }
        this.setDaemon(true);
        this.coreThreadsCount = threadsCount;
        this.isActive.set(true);
        this.threadWorkers = new ThreadWorker[threadsCount];
        for (int i = 0; i < threadsCount; ++i) {
            final ThreadWorker worker = new ThreadWorker(this, i);
            worker.start();
            this.threadWorkers[i] = worker;
        }
    }
    
    public boolean excute(final Runnable paramRunnable) {
        if (!this.isActive.get()) {
            MyThreadPool.LOG.warn((Object)String.format("Can not excute the task:[%s], the thread pool my be shut down yet.", paramRunnable.toString()));
            return false;
        }
        if (paramRunnable != null) {
            synchronized (this.waitQueue) {
                this.waitedTaskCount.incrementAndGet();
                this.waitQueue.addLast(paramRunnable);
            }
        }
        return true;
    }
    
    public Runnable receiveTask() {
        while (this.waitQueue.size() <= 0 && this.isActive.get()) {
            try {
                Thread.sleep(10000L);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        synchronized (this.waitQueue) {
            if (this.waitQueue.size() > 0) {
                this.waitedTaskCount.decrementAndGet();
                return this.waitQueue.removeFirst();
            }
        }
        return null;
    }
    
    public int getWaitTaskCount() {
        if (null == this.waitQueue) {
            return 0;
        }
        return this.waitedTaskCount.get();
    }
    
    public int capacity() {
        return this.coreThreadsCount;
    }
    
    public boolean isActive() {
        return this.isActive.get();
    }
    
    public void shutDown() {
        try {
            if (this.isActive.get()) {
                MyThreadPool.LOG.info((Object)"The thread pool is to shut down. Waiting the tasks in the queue, and the running task to be complete.");
                this.isActive.set(false);
                this.interrupt();
                int tempThreadRunnedCount = this.waitedTaskCount.get();
                while (true) {
                    Thread.sleep(10000L);
                    final int activeThreadCount = this.activeCount();
                    final Thread[] list = new Thread[activeThreadCount];
                    this.enumerate(list);
                    boolean noMoreFetcherThread = true;
                    for (int i = 0; i < activeThreadCount; ++i) {
                        if (list[i] != null) {
                            final String threadName = list[i].getName();
                            if (threadName.startsWith("com.wintim.service")) {
                                noMoreFetcherThread = false;
                            }
                            if (MyThreadPool.LOG.isDebugEnabled()) {
                                MyThreadPool.LOG.debug((Object)list[i].toString());
                            }
                        }
                    }
                    if (noMoreFetcherThread) {
                        if (MyThreadPool.LOG.isDebugEnabled()) {
                            MyThreadPool.LOG.debug((Object)("number of active threads: " + activeThreadCount));
                        }
                        if (tempThreadRunnedCount == this.waitedTaskCount.get()) {
                            break;
                        }
                        tempThreadRunnedCount = this.waitedTaskCount.get();
                    }
                    if (MyThreadPool.traceThread) {
                        ThreadTracer.printAllThread();
                    }
                }
                this.waitQueue.clear();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            MyThreadPool.LOG.fatal((Object)"Failed to shut down the thread pool");
            if (MyThreadPool.traceThread) {
                ThreadTracer.printAllThread();
            }
            MyThreadPool.LOG.warn((Object)"Destroy the thread pool!");
            this.destroy();
        }
        this.waitQueue = null;
    }
    
    static {
        LOG = LogFactory.getLog(MyThreadPool.class.getCanonicalName());
        traceThread = ConfigUtils.getConfig().getBoolean("thread.pool.trace.thread", false);
        MyThreadPool.id = 0;
    }
}
