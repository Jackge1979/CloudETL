package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.ipc.*;
import java.io.*;
import org.apache.hadoop.hdfs.protocol.*;
import java.util.*;

public class Manager
{
    private JobSubmissionProtocol jobProtocol;
    private ClientProtocol clientProtocol;
    public static final int TASK_ACTIVE = 0;
    public static final int TASK_STOP = -1;
    
    public Manager(final Configuration conf) throws IOException, InterruptedException {
        this.jobProtocol = (JobSubmissionProtocol)RPC.getProxy((Class)JobSubmissionProtocol.class, 28L, JobTracker.getAddress(conf), conf);
    }
    
    public long[] getStats() throws IOException {
        return this.clientProtocol.getStats();
    }
    
    public DatanodeInfo[] getLiveDatanode() throws IOException {
        return this.clientProtocol.getDatanodeReport(HdfsConstants.DatanodeReportType.LIVE);
    }
    
    public DatanodeInfo[] getDeadDatanode() throws IOException {
        return this.clientProtocol.getDatanodeReport(HdfsConstants.DatanodeReportType.DEAD);
    }
    
    public JobQueueInfo[] getQueues() throws IOException {
        return this.jobProtocol.getQueues();
    }
    
    public Counters getJobCounters(final JobID jobid) throws IOException {
        return this.jobProtocol.getJobCounters(jobid);
    }
    
    public String getSystemDir() {
        return this.jobProtocol.getSystemDir();
    }
    
    public ClusterStatus getClusterStatus(final boolean detailed) throws IOException {
        return this.jobProtocol.getClusterStatus(detailed);
    }
    
    public TaskReport[] getMapTaskReports(final JobID jobid) throws IOException {
        return this.jobProtocol.getMapTaskReports(jobid);
    }
    
    public TaskReport[] getReduceTaskReports(final JobID jobid) throws IOException {
        return this.jobProtocol.getReduceTaskReports(jobid);
    }
    
    public TaskReport[] getCleanupTaskReports(final JobID jobid) throws IOException {
        return this.jobProtocol.getCleanupTaskReports(jobid);
    }
    
    public TaskReport[] getSetupTaskReports(final JobID jobid) throws IOException {
        return this.jobProtocol.getSetupTaskReports(jobid);
    }
    
    public JobProfile getJobProfile(final JobID jobid) throws IOException {
        return this.jobProtocol.getJobProfile(jobid);
    }
    
    public JobStatus[] getAllJobs() throws IOException {
        return this.jobProtocol.getAllJobs();
    }
    
    public void killJob(final String jobId) throws IOException {
        this.killJob(this.getJobId(jobId));
    }
    
    public void killJob(final JobID jobID) throws IOException {
        this.jobProtocol.killJob(jobID);
    }
    
    private JobID getJobId(final String jobID) {
        return JobID.forName(jobID);
    }
    
    public JobStatus getJobStatus(final String jobId) throws IOException {
        return this.jobProtocol.getJobStatus(this.getJobId(jobId));
    }
    
    public void killJob(final JobStatus job) throws IOException {
        this.killJob(job.getJobID());
    }
    
    public JobStatus[] getRunningJob() throws IOException {
        return this.jobProtocol.jobsToComplete();
    }
    
    public List<JobStatus> getSucceededJob() throws IOException {
        return this.getJobs(2);
    }
    
    public List<JobStatus> getFailedJob() throws IOException {
        return this.getJobs(3);
    }
    
    public List<JobStatus> getPrepJob() throws IOException {
        return this.getJobs(4);
    }
    
    public List<JobStatus> getKilledJob() throws IOException {
        return this.getJobs(5);
    }
    
    private List<JobStatus> getJobs(final int status) throws IOException {
        final List<JobStatus> jobs = new ArrayList<JobStatus>();
        final JobStatus[] arr$;
        final JobStatus[] allJobs = arr$ = this.getAllJobs();
        for (final JobStatus job : arr$) {
            if (job.getRunState() == status) {
                jobs.add(job);
            }
        }
        return this.reList(jobs);
    }
    
    private List<JobStatus> reList(final List<JobStatus> jobs) {
        return jobs.isEmpty() ? null : jobs;
    }
    
    public static void main(final String[] arg0) {
    }
}
