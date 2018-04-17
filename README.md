# 1. Introduction
**IPM-Scheduler** is short for Impala Pool Memory Scheduler,
which can adjust memory allcation of each pool dynamically.
It is based on historical query data from impala within a certain period of time.
Through intelligently adjusting pool resource allocation, IPM-Scheduler can effectively reduce the waiting time of queries, and improve memory utilization.
According to our scenario, the total waiting time is reduced by 70%-80%.  


# 2. Installation
## 2.1. Dependencies
 - Impala ( >=impala-2.5.0+cdh5.7.2 )
 - Cloudera Manager ( >=cdh5.7.2 )
 - Python3 ( >= 3.5 )
 
**Important: Testing OK on CDH 5.7.2 and 5.12.1, other versions are not guaranteed to be available.**


## 2.2. Install
 >
    $ git clone git@gitlab.gridsum.com:data-engineering/impala-toolbox/IPM-scheduler.git
    $ cd IPM-scheduler
    $ python3 setup.py install
    $ echo "export SCHEDULER_HOME=\`pwd\`" >> ~/.bashrc  

## 2.3. Edit config of scheduler
 - You must edit the config of **cloudera_manager** and **pool** in [the config file](./conf/scheduler.yml). In addition, you can refer to the config instructions to modify other config items as needed.

## 2.4. Start/Stop scheduler daemon
 - Start daemon: 
> 
     $ ./bin/scheduler_daemon.sh start
 
 - Stop daemon: 
> 
     $ ./bin/scheduler_daemon.sh stop


# 3. Tutorials & Documentation

## 3.1. Principle
The scheduling principle is mainly as follows:
 - First, crawls a certain period of time historical query information from Cloudera Manager.
 - Then, generates a memory resource allocation plan for each pool according to scheduler config, impala config and historical query information.
 - Finally, executes the memory resource allocation plan by modifying the impala config through Cloudera Manager.

## 3.2. [Default scheduling strategy](./scheduler/priority_schedule.py)

## 3.3. You can implement your specific scheduling strategy
  - [Schedule strategy interface](./scheduler/base_schedule.py)
  - [Example schedule strategy](./scheduler/example_schedule.py)
  - Edit [the config file](./conf/scheduler.yml)
   
   > 
     schedule_module_name: 'scheduler'
     schedule_py_name: 'example_schedule'`
     schedule_class_name: 'DoNothing1Schedule'

## 3.4. Send schedule report when scheduling
 - Edit [the config file](./conf/scheduler.yml)
  - edit config items about **email**
  - edit config item: 

    >
      enable_schedule_report: true 

## 3.5. Utils
 - Backup impala config: 
>
     $ ./bin/scheduler_utils.sh backup

 - Rollback impala config: 
> 
     $ ./bin/scheduler_utils.sh rollback

 - Check [the scheduler config file](./conf/scheduler.yml): 
> 
     $ ./bin/scheduler_utils.sh check

# 4. Communication
  impala-toolbox-help@gridsum.com

# 5. License
IPM-Scheduler is [licensed under the Apache License 2.0.](./LICENSE)


