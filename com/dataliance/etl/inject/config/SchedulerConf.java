package com.dataliance.etl.inject.config;

import org.apache.hadoop.conf.*;
import java.util.*;
/*
 * MapReduce 调度器
 */
class SchedulerConf extends DAConf
{
    private static final Set<String> keys;
    
    public SchedulerConf(final Configuration conf, final String resource) {
        super(conf, resource);
        this.setFilter(new Filter() {
            @Override
            public boolean accept(final String name, final String value) {
                return SchedulerConf.keys.contains(name);
            }
        });
    }
    
    @Override
    public void set(final String name, final String value) {
        if (SchedulerConf.keys.contains(name)) {
            super.set(name, value);
        }
    }
    
    static {
    		// 抢占设置
        (keys = new HashSet<String>()).add("mapred.fairscheduler.preemption");
        // 调度资源池
        SchedulerConf.keys.add("mapred.fairscheduler.pool");
        // 资源池命名属性
        SchedulerConf.keys.add("mapred.fairscheduler.poolnameproperty");
        // 分配文件
        SchedulerConf.keys.add("mapred.fairscheduler.allocation.file");
        // s
        SchedulerConf.keys.add("mapred.fairscheduler.sizebasedweight");
        SchedulerConf.keys.add("mapred.fairscheduler.preemption.only.log");
        // 更新间隔
        SchedulerConf.keys.add("mapred.fairscheduler.update.interval");
        SchedulerConf.keys.add("mapred.fairscheduler.preemption.interval");
        // 权重调节
        SchedulerConf.keys.add("mapred.fairscheduler.weightadjuster");
        // 负载管理
        SchedulerConf.keys.add("mapred.fairscheduler.loadmanager");
        // 任务选取
        SchedulerConf.keys.add("mapred.fairscheduler.taskselector");
    }
}
