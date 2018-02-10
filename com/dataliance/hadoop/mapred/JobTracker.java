package com.dataliance.hadoop.mapred;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import java.util.regex.*;

import com.dataliance.util.*;
import com.dataliance.hadoop.mapred.vo.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;

public class JobTracker
{
    private static Configuration conf;
    private static Manager manager;
    static final String KEY = "(\\w+)";
    static final String VALUE = "[^\"\\\\]*+(?:\\\\.[^\"\\\\]*+)*+";
    static final Pattern pattern;
    static final char LINE_DELIMITER_CHAR = '.';
    static final char[] charsToEscape;
    
    public JobTracker(final Configuration conf) throws IOException, InterruptedException {
        JobTracker.conf = conf;
        if (JobTracker.manager == null) {
            JobTracker.manager = new Manager(conf);
        }
    }
    
    public static void main(final String[] args) {
        try {
            final JobTracker jt = new JobTracker(DAConfigUtil.create());
            final ClusterStatus stats = jt.getClusterStatus();
            System.out.println("--" + stats.getNodes());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public List<JobStatus> getRunningJobs() throws IOException {
        final ArrayList<JobStatus> runningList = new ArrayList<JobStatus>();
        final org.apache.hadoop.mapred.JobStatus[] runningJob = JobTracker.manager.getRunningJob();
        if (runningJob.length > 0) {
            for (int j = 0; j < runningJob.length; ++j) {
                final org.apache.hadoop.mapred.JobStatus job = runningJob[j];
                runningList.add(this.transmit(job));
            }
        }
        Collections.sort(runningList, new ComparatorJob());
        return runningList;
    }
    
    public List<JobStatus> getCompletedJobs() throws IOException {
        final ArrayList<JobStatus> completedList = new ArrayList<JobStatus>();
        final List<org.apache.hadoop.mapred.JobStatus> completeJob = JobTracker.manager.getSucceededJob();
        if (completeJob.size() > 0) {
            for (final org.apache.hadoop.mapred.JobStatus job : completeJob) {
                completedList.add(this.transmit(job));
            }
        }
        Collections.sort(completedList, new ComparatorJob());
        return completedList;
    }
    
    public List<JobStatus> getFailedJobs() throws IOException {
        final ArrayList<JobStatus> failedList = new ArrayList<JobStatus>();
        final List<org.apache.hadoop.mapred.JobStatus> failedJob = JobTracker.manager.getFailedJob();
        if (failedJob.size() > 0) {
            for (final org.apache.hadoop.mapred.JobStatus job : failedJob) {
                failedList.add(this.transmit(job));
            }
        }
        Collections.sort(failedList, new ComparatorJob());
        return failedList;
    }
    
    public JobStatus transmit(final org.apache.hadoop.mapred.JobStatus job) throws IOException {
        final JobStatus myJob = new JobStatus();
        final JobProfile jobProfile = JobTracker.manager.getJobProfile(job.getJobID());
        final TaskReport[] mapReport = JobTracker.manager.getMapTaskReports(job.getJobID());
        int succMap = 0;
        for (int i = 0; i < mapReport.length; ++i) {
            final TaskReport tr = mapReport[i];
            if (tr.getCurrentStatus().toString().equals("COMPLETE")) {
                ++succMap;
            }
        }
        final TaskReport[] reduceReport = JobTracker.manager.getReduceTaskReports(job.getJobID());
        int succReduce = 0;
        for (int j = 0; j < reduceReport.length; ++j) {
            final TaskReport tr2 = reduceReport[j];
            if (tr2.getCurrentStatus().toString().equals("COMPLETE")) {
                ++succReduce;
            }
        }
        myJob.init(job);
        myJob.setNumMaps(mapReport.length);
        myJob.setNumReduce(reduceReport.length);
        myJob.setCompletedMaps(succMap);
        myJob.setCompletedReduces(succReduce);
        myJob.setJobName(jobProfile.getJobName());
        return myJob;
    }
    
    public ClusterStatus getClusterStatus() throws IOException {
        final org.apache.hadoop.mapred.ClusterStatus clustersta = JobTracker.manager.getClusterStatus(true);
        final ClusterStatus cs = new ClusterStatus();
        cs.setRunningMapTasks(clustersta.getMapTasks());
        cs.setRunningReduceTasks(clustersta.getReduceTasks());
        cs.setTotalSumit(JobTracker.manager.getAllJobs().length);
        cs.setNodes(clustersta.getTaskTrackers());
        cs.setMapTaskCapacity(clustersta.getMaxMapTasks());
        cs.setReduceTaskCapacity(clustersta.getMaxReduceTasks());
        cs.setBlacklistNodes(clustersta.getBlacklistedTrackers());
        cs.setExcludedNodes(clustersta.getNumExcludedNodes());
        cs.setUsedMemory(clustersta.getUsedMemory());
        cs.setMaxMemory(clustersta.getMaxMemory());
        return cs;
    }
    
    static void parseLine(final String line, final JobHistory.Listener l) throws IOException {
        final int idx = line.indexOf(32);
        final String recType = line.substring(0, idx);
        final String data = line.substring(idx + 1, line.length());
        final Matcher matcher = JobTracker.pattern.matcher(data);
        final Map<JobHistory.Keys, String> parseBuffer = new HashMap<JobHistory.Keys, String>();
        while (matcher.find()) {
            final String tuple = matcher.group(0);
            final String[] parts = StringUtils.split(tuple, '\\', '=');
            final String value = parts[1].substring(1, parts[1].length() - 1);
            parseBuffer.put(JobHistory.Keys.valueOf(parts[0]), value);
        }
        l.handle(JobHistory.RecordTypes.valueOf(recType), (Map)parseBuffer);
        parseBuffer.clear();
    }
    
    public Map<DACloudKeys, String> countInfo(final TaskInfoManager mapT) {
        final Map<DACloudKeys, String> map = new HashMap<DACloudKeys, String>();
        final List<String> stats = mapT.pairs.get(JobHistory.Keys.TASK_STATUS);
        final int total_map = stats.size();
        int succ_map = 0;
        int failed_map = 0;
        int killed_map = 0;
        for (final String str : stats) {
            if (str.equals(JobHistory.Values.SUCCESS.toString())) {
                ++succ_map;
            }
            else if (str.equals(JobHistory.Values.KILLED.toString())) {
                ++killed_map;
            }
            else {
                if (!str.equals(JobHistory.Values.FAILED.toString())) {
                    continue;
                }
                ++failed_map;
            }
        }
        map.put(DACloudKeys.SUCCESSED, succ_map + "");
        map.put(DACloudKeys.TOTALTASKS, total_map + "");
        map.put(DACloudKeys.FAILED, failed_map + "");
        map.put(DACloudKeys.KILLED, killed_map + "");
        long startTime = 0L;
        long endTime = 0L;
        final List<String> start = mapT.pairs.get(JobHistory.Keys.START_TIME);
        for (final String str2 : start) {
            if (startTime == 0L || startTime > Long.parseLong(str2)) {
                startTime = Long.parseLong(str2);
            }
        }
        final List<String> end = mapT.pairs.get(JobHistory.Keys.FINISH_TIME);
        for (final String str3 : end) {
            if (endTime < Long.parseLong(str3)) {
                endTime = Long.parseLong(str3);
            }
        }
        map.put(DACloudKeys.STARTTIME, startTime + "");
        map.put(DACloudKeys.FINISHTIME, endTime + "");
        return map;
    }
    
    public HistoryJobProfile getHistoryJobProfile(final String jobId) {
        final File file = this.getHistoryFile(jobId);
        BufferedReader reader = null;
        final HistoryJobProfile hjob = new HistoryJobProfile();
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            final JobInfoManager jmgr = new JobInfoManager();
            final TaskInfoManager mapT = new TaskInfoManager("MAP");
            final TaskInfoManager setupT = new TaskInfoManager("SETUP");
            final TaskInfoManager reduceT = new TaskInfoManager("REDUCE");
            final TaskInfoManager cleanupT = new TaskInfoManager("CLEANUP");
            while ((tempString = reader.readLine()) != null) {
                jmgr.JobInfoManage(tempString);
                mapT.TaskInfoManage(tempString);
                setupT.TaskInfoManage(tempString);
                reduceT.TaskInfoManage(tempString);
                cleanupT.TaskInfoManage(tempString);
            }
            reader.close();
            hjob.setUser(jmgr.pairs.get(JobHistory.Keys.USER));
            hjob.setJobFile(jmgr.pairs.get(JobHistory.Keys.JOBCONF));
            hjob.setJobid(jmgr.pairs.get(JobHistory.Keys.JOBID));
            hjob.setName(jmgr.pairs.get(JobHistory.Keys.JOBNAME));
            hjob.setStartTime(jmgr.pairs.get(JobHistory.Keys.LAUNCH_TIME));
            hjob.setSubmitTime(jmgr.pairs.get(JobHistory.Keys.SUBMIT_TIME));
            hjob.setFinishedTime(jmgr.pairs.get(JobHistory.Keys.FINISH_TIME));
            hjob.setStatus(jmgr.pairs.get(JobHistory.Keys.JOB_STATUS));
            final Map<DACloudKeys, String> setup = this.countInfo(setupT);
            final Map<DACloudKeys, String> map = this.countInfo(mapT);
            final Map<DACloudKeys, String> reduce = this.countInfo(reduceT);
            final Map<DACloudKeys, String> cleanup = this.countInfo(cleanupT);
            hjob.setMap(map);
            hjob.setReduce(reduce);
            hjob.setCleanup(cleanup);
            hjob.setSetup(setup);
            System.out.println("ok");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException ex) {}
            }
        }
        return hjob;
    }
    
    public com.dataliance.hadoop.mapred.vo.JobProfile getJobProfile(final String jobId) throws IOException {
        final com.dataliance.hadoop.mapred.vo.JobProfile myjp = new com.dataliance.hadoop.mapred.vo.JobProfile();
        final JobID jobIdObj = JobID.forName(jobId);
        final JobProfile jobProfile = JobTracker.manager.getJobProfile(jobIdObj);
        final org.apache.hadoop.mapred.JobStatus job = JobTracker.manager.getJobStatus(jobId);
        final float mapPro = job.mapProgress();
        final float reducePro = job.reduceProgress();
        long finishtime = 0L;
        final TaskReport[] mapReport = JobTracker.manager.getMapTaskReports(jobIdObj);
        final TaskReport[] reduceReport = JobTracker.manager.getReduceTaskReports(jobIdObj);
        final TaskReport[] setupReport = JobTracker.manager.getCleanupTaskReports(jobIdObj);
        final TaskReport[] cleanupReport = JobTracker.manager.getCleanupTaskReports(jobIdObj);
        int succMap = 0;
        int runningMap = 0;
        int pendingMap = 0;
        int killedMap = 0;
        int succReduce = 0;
        int runningReduce = 0;
        int pendingReduce = 0;
        int killedReduce = 0;
        if (mapReport.length != 0 || reduceReport.length != 0 || setupReport.length != 0 || cleanupReport.length != 0) {
            for (int i = 0; i < mapReport.length; ++i) {
                final TaskReport tr = mapReport[i];
                if (tr.getCurrentStatus().toString().equals("COMPLETE")) {
                    ++succMap;
                }
                if (tr.getCurrentStatus().toString().equals("RUNNING")) {
                    ++runningMap;
                }
                if (tr.getCurrentStatus().toString().equals("PENDING")) {
                    ++pendingMap;
                }
                if (tr.getCurrentStatus().toString().equals("KILLED")) {
                    ++killedMap;
                }
            }
            for (int i = 0; i < reduceReport.length; ++i) {
                final TaskReport tr = reduceReport[i];
                if (tr.getCurrentStatus().toString().equals("COMPLETE")) {
                    ++succReduce;
                }
                if (tr.getCurrentStatus().toString().equals("RUNNING")) {
                    ++runningReduce;
                }
                if (tr.getCurrentStatus().toString().equals("PENDING")) {
                    ++pendingReduce;
                }
                if (tr.getCurrentStatus().toString().equals("KILLED")) {
                    ++killedReduce;
                }
            }
            for (final TaskReport tr2 : setupReport) {
                if (tr2.getStartTime() != 0L) {
                    myjp.setJobSetup(tr2.getCurrentStatus().toString());
                }
            }
            for (final TaskReport tr2 : cleanupReport) {
                if (tr2.getFinishTime() > finishtime) {
                    finishtime = tr2.getFinishTime();
                }
                if (tr2.getStartTime() != 0L) {
                    myjp.setJobCleanup(tr2.getCurrentStatus().toString());
                }
            }
        }
        myjp.setFinishedTime(finishtime);
        myjp.setName(jobProfile.getJobName());
        myjp.setJobid(jobIdObj);
        myjp.setUser(jobProfile.getUser());
        myjp.setJobFile(jobProfile.getJobFile());
        myjp.setUrl(jobProfile.getURL());
        myjp.setStartTime(job.getStartTime());
        myjp.setStatus(org.apache.hadoop.mapred.JobStatus.getJobRunState(job.getRunState()));
        myjp.setMapprocess(mapPro);
        myjp.setMapTasks(mapReport.length);
        myjp.setMapPending(pendingMap);
        myjp.setMapRunning(runningMap);
        myjp.setMapComplete(succMap);
        myjp.setMapKill(killedMap);
        myjp.setReduceProcess(reducePro);
        myjp.setReduceTasks(reduceReport.length);
        myjp.setReduceComplete(succReduce);
        myjp.setReduceKill(killedReduce);
        myjp.setReducePending(pendingReduce);
        myjp.setReduceRunning(runningReduce);
        return myjp;
    }
    
    public Counters getJobCounters(final String jobId) throws IOException {
        final JobID jobid = JobID.forName(jobId);
        final Counters counters = JobTracker.manager.getJobCounters(jobid);
        return counters;
    }
    
    public Map<String, long[]> getLogs(String path) {
        path = ((path != null) ? path : "");
        final Map<String, long[]> properties = new HashMap<String, long[]>();
        String logPath = JobTracker.conf.get("logpath");
        logPath = logPath + "/" + path;
        final File log = new File(logPath);
        final String[] files = log.list();
        for (int i = 0; i < files.length; ++i) {
            String fileName = files[i];
            final File subFile = new File(logPath + "/" + fileName);
            if (subFile.isDirectory()) {
                fileName += "/";
            }
            final long filelength = subFile.length();
            final long lastmodified = subFile.lastModified();
            final long[] arrPro = { filelength, lastmodified };
            properties.put(fileName, arrPro);
        }
        return properties;
    }
    
    public InputStream getLog(String fileName) {
        final String logPath = JobTracker.conf.get("logpath");
        fileName = logPath + "/" + fileName;
        if (fileName == null || fileName.equals("")) {
            return null;
        }
        final File file = new File(fileName);
        InputStream in = null;
        try {
            in = new FileInputStream(file);
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return in;
    }
    
    public static List<String> getAllFiles(final String bootPath) {
        final List<String> files = new ArrayList<String>();
        final File parentDir = new File(bootPath);
        final String[] arr$;
        final String[] list = arr$ = parentDir.list();
        for (final String s : arr$) {
            final String name = bootPath + "/" + s;
            final File instance = new File(name);
            if (instance.isFile()) {
                files.add(name);
            }
            else {
                files.addAll(getAllFiles(name));
            }
        }
        return files;
    }
    
    public File getHistoryFile(final String jobId) {
        final String bootPath = JobTracker.conf.get("logpath");
        final List<String> files = getAllFiles(bootPath);
        for (final String filePath : files) {
            if (filePath.indexOf(jobId) >= 0 && !filePath.endsWith(".xml") && !filePath.endsWith(".crc")) {
                return new File(filePath);
            }
        }
        return null;
    }
    
    public JobQueueInfo[] getQueues() throws IOException {
        return JobTracker.manager.getQueues();
    }
    
    static {
        pattern = Pattern.compile("(\\w+)=\"[^\"\\\\]*+(?:\\\\.[^\"\\\\]*+)*+\"");
        charsToEscape = new char[] { '\"', '=', '.' };
    }
    
    static class JobInfoManager implements JobHistory.Listener
    {
        private long version;
        private KeyValuePair pairs;
        
        JobInfoManager() {
            this.version = 0L;
            this.pairs = new KeyValuePair();
        }
        
        public void JobInfoManage(final String line) throws IOException {
            if (null != line) {
                JobTracker.parseLine(line, (JobHistory.Listener)this);
            }
        }
        
        public void handle(final JobHistory.RecordTypes recType, final Map<JobHistory.Keys, String> values) throws IOException {
            if (JobHistory.RecordTypes.Job == recType) {
                this.pairs.handle(values);
            }
        }
    }
    
    public enum DACloudKeys
    {
        TOTALTASKS, 
        SUCCESSED, 
        KILLED, 
        FAILED, 
        STARTTIME, 
        FINISHTIME;
    }
    
    static class TaskInfoManager implements JobHistory.Listener
    {
        private long version;
        private DACloudMap pairs;
        private String taskType;
        
        public TaskInfoManager(final String taskType) {
            this.version = 0L;
            this.pairs = new DACloudMap();
            this.taskType = taskType;
        }
        
        public void TaskInfoManage(final String line) throws IOException {
            if (null != line) {
                JobTracker.parseLine(line, (JobHistory.Listener)this);
            }
        }
        
        public void handle(final JobHistory.RecordTypes recType, final Map<JobHistory.Keys, String> values) throws IOException {
            if (JobHistory.RecordTypes.Task == recType && values.get(JobHistory.Keys.TASK_TYPE).equals(this.taskType)) {
                this.pairs.handle(values);
            }
        }
    }
    
    static class KeyValuePair
    {
        private Map<JobHistory.Keys, String> values;
        
        KeyValuePair() {
            this.values = new HashMap<JobHistory.Keys, String>();
        }
        
        public String get(final JobHistory.Keys k) {
            final String s = this.values.get(k);
            return (s == null) ? "" : s;
        }
        
        public int getInt(final JobHistory.Keys k) {
            final String s = this.values.get(k);
            if (null != s) {
                return Integer.parseInt(s);
            }
            return 0;
        }
        
        public long getLong(final JobHistory.Keys k) {
            final String s = this.values.get(k);
            if (null != s) {
                return Long.parseLong(s);
            }
            return 0L;
        }
        
        public void set(final JobHistory.Keys k, final String s) {
            this.values.put(k, s);
        }
        
        public void set(final Map<JobHistory.Keys, String> m) {
            this.values.putAll(m);
        }
        
        public synchronized void handle(final Map<JobHistory.Keys, String> values) {
            this.set(values);
        }
        
        public Map<JobHistory.Keys, String> getValues() {
            return this.values;
        }
    }
    
    static class DACloudMap
    {
        private Map<JobHistory.Keys, List<String>> values;
        
        DACloudMap() {
            this.values = new HashMap<JobHistory.Keys, List<String>>();
        }
        
        public List<String> get(final JobHistory.Keys k) {
            final List<String> s = this.values.get(k);
            return s;
        }
        
        public void set(final JobHistory.Keys k, final String s) {
            if (this.values.get(k) != null) {
                final List<String> v = this.values.get(k);
                v.add(s);
                this.values.put(k, v);
            }
            else {
                final List<String> v = new ArrayList<String>();
                v.add(s);
                this.values.put(k, v);
            }
        }
        
        public void set(final Map<JobHistory.Keys, String> map) {
            for (final Map.Entry entry : map.entrySet()) {
                final JobHistory.Keys key = entry.getKey();
                final String val = entry.getValue();
                this.set(key, val);
            }
        }
        
        public synchronized void handle(final Map<JobHistory.Keys, String> values) {
            this.set(values);
        }
    }
    
    class ComparatorJob implements Comparator
    {
        @Override
        public int compare(final Object arg0, final Object arg1) {
            final JobStatus job0 = (JobStatus)arg0;
            final JobStatus job2 = (JobStatus)arg1;
            int flag = 0;
            if (new Long(job0.getStartTime()).compareTo(new Long(job2.getStartTime())) < 0) {
                flag = 1;
            }
            else if (new Long(job0.getStartTime()).compareTo(new Long(job2.getStartTime())) > 0) {
                flag = -1;
            }
            if (flag == 0) {
                return -job0.getJobidStr().compareTo(job2.getJobidStr());
            }
            return flag;
        }
    }
}
