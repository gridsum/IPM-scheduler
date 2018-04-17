import logging
from abc import ABCMeta, abstractmethod
import time

from scheduler.constants import FormativeQueryInfoColumn
from scheduler.constants import PoolSectOpts

LOGGER = logging.getLogger(__name__)


class ScheduleInterface(metaclass=ABCMeta):
    """
    The ScheduleInterface abstract base class that provides methods for the statistics and the allocated
    memory of the pool participating in the scheduling.
    """
    @abstractmethod
    def get_pools_stat(self, fetched_query_info, start_time, end_time):
        """
        Get the statistics of the pool participating in the scheduling.

        :param fetched_query_info: (DataFrame) The fetched query information. Columns as follow:
            ["query_id", "pool", "start_time", "admission_wait", "duration_millis", "mem_limit", "max_host"]
        :param start_time: (datetime) The start time to fetching query information.
        :param end_time: (datetime) The end time to fetching query information.
        :return: (dict) A dict object mapping pool name to a PoolStat object.
            For example:
                {"root.test_pool1": PoolStat("", 10, 10, 10, 0, 500, 0),
                 "root.test_pool2": PoolStat("", 10, 10, 10, 10, 1000, 500),
                 "root.test_pool3": PoolStat("", 10, 10, 10, 0, 500, 0)}
        """
        pass

    @abstractmethod
    def get_pools_allocated_mem(self, section_schedule, pools_info):
        """
        Get the allocated memory of the pool participating in the scheduling.

        :param section_schedule: (dict) The schedule section of configuration in ../conf/scheduler.yml.
            For example:
                {"schedule_available_impalad_threshold": 1,
                 "schedule_interval_minutes": 30,
                 "schedule_memory_unit": 1024,
                 "free_memory_schedule_ratio": 0.8,
                 "busy_pool_threshold_seconds": 10}
        :param pools_info: (dict) The configuration and statistics of the pool participating in the
            scheduling.
            For example:
                {"root.test_pool1": PoolInfo("", 1000.0, 1.0, 0, 2000, PoolStat("", 10, 10, 10, 0, 500, 0)),
                 "root.test_pool2": PoolInfo("", 1000.0, 1.0, 0, 2000, PoolStat("", 10, 10, 10, 10, 1000, 500)),
                 "root.test_pool3": PoolInfo("", 1000.0, 1.0, 0, 2000, PoolStat("", 10, 10, 10, 0, 500, 0))}
        :return: (dict) A dict object mapping pool name to the allocated memory.
            For example:
                {"root.test_pool1": 600
                 "root.test_pool2": 1500
                 "root.test_pool3": 900}
        """
        pass


class AbstractSchedule(ScheduleInterface):
    """
    The AbstractSchedule abstract class that provides methods for the statistics and the allocated memory of the pool
    participating in the scheduling, which extends from ScheduleInterface and implements the method
    get_pools_stat.
    """

    @classmethod
    def get_pools_stat(cls, queries_info, start_time, end_time):
        if queries_info is None:
            return None
        stat_start_milli_sec = int(time.mktime(start_time.timetuple()) * 1000)
        stat_end_milli_sec = int(time.mktime(end_time.timetuple()) * 1000)
        pools_stat = {}
        for pool_name, pool_group in queries_info.groupby(FormativeQueryInfoColumn.POOL):
            pool_group_sort = pool_group.sort_values(by=FormativeQueryInfoColumn.START_TIME)

            query_total, wait_query_total, wait_mem_total, used_mem_total = 0, 0, 0, 0
            wait_milli_secs, run_milli_secs, wait_milli_sec_cursor, run_sec_cursor = 0, 0, 0, 0
            for _, row in pool_group_sort.iterrows():
                start = row.get(FormativeQueryInfoColumn.START_TIME)
                start_milli_sec = int(time.mktime(start.timetuple()) * 1000)
                queued_milli_secs = int(row.get(FormativeQueryInfoColumn.ADMISSION_WAIT))
                duration_milli_sec = row.get(FormativeQueryInfoColumn.DURATION_MILLIS)
                mem_limit = row.get(FormativeQueryInfoColumn.MEM_LIMIT)
                hosts = row.get(FormativeQueryInfoColumn.MAX_HOST)
                used_mem = mem_limit * hosts
                end_milli_sec = start_milli_sec + duration_milli_sec + queued_milli_secs
                query_total += 1
                if queued_milli_secs > 0:
                    wait_query_total += 1

                wait_start_milli_sec = max(start_milli_sec, stat_start_milli_sec)
                wait_end_milli_sec = min(start_milli_sec + queued_milli_secs, stat_end_milli_sec)
                if wait_end_milli_sec > wait_start_milli_sec:
                    wait_mem_total += used_mem * (wait_end_milli_sec - wait_start_milli_sec)

                difference = wait_end_milli_sec - max(wait_start_milli_sec, wait_milli_sec_cursor)
                if difference > 0:
                    wait_milli_secs += difference
                    wait_milli_sec_cursor = wait_end_milli_sec

                run_start_milli_sec = max(start_milli_sec + queued_milli_secs, stat_start_milli_sec)
                run_end_milli_sec = min(end_milli_sec, stat_end_milli_sec)
                used_mem_total += used_mem * (run_end_milli_sec - run_start_milli_sec)

                difference = run_end_milli_sec - max(run_start_milli_sec, run_sec_cursor)
                if difference > 0:
                    run_milli_secs += difference
                    run_sec_cursor = run_end_milli_sec

            wait_mem_avg = 0 if wait_milli_secs == 0 else wait_mem_total / wait_milli_secs
            used_mem_avg = 0 if run_milli_secs == 0 else used_mem_total / run_milli_secs
            pool_stat = PoolStat(
                pool_name, query_total, wait_query_total, run_milli_secs / 1000,
                wait_milli_secs / 1000, int(used_mem_avg), int(wait_mem_avg))
            pools_stat[pool_name] = pool_stat
        LOGGER.info("pools stat info: %s", pools_stat)
        return pools_stat

    @classmethod
    def get_pools_allocated_mem(cls, section_schedule, pools_info):
        pass


class PoolStat(object):
    """
    The PoolStat class that provides encapsulation for the statistics of the pool participating in
    the scheduling.
    """
    def __init__(self, pool_name="", query_total=0, wait_query_total=0,
                 run_secs=0, wait_secs=0, used_mem_avg=0, wait_mem_avg=0):
        """
        Create a PoolStat object to encapsulating statistics for the pool.

        :param pool_name: (str) The pool name. By default, pool_name is "".
        :param query_total: (int) The total query number.
        :param wait_query_total: (int) The wait query number.
        :param run_secs: (int) The run seconds for total query.
        :param wait_secs: (int) The wait seconds for wait query.
        :param used_mem_avg: (int) The average used memory.
        :param wait_mem_avg: (int) The average wait memory.
        """
        self.pool_name = pool_name
        self.query_total = query_total
        self.wait_query_total = wait_query_total
        self.run_secs = run_secs
        self.wait_secs = wait_secs
        self.used_mem_avg = used_mem_avg
        self.wait_mem_avg = wait_mem_avg

    def __str__(self):
        return "(PoolStat: {pool_name:%s, query_total:%s, wait_query_total:%s, run_secs:%s, " \
               "wait_secs:%s, used_mem_avg:%s, wait_mem_avg:%s})" % \
               (self.pool_name, self.query_total, self.wait_query_total,
                self.run_secs, self.wait_secs, self.used_mem_avg, self.wait_mem_avg)

    __repr__ = __str__


class PoolInfo(object):
    """
    The PoolInfo class that provides encapsulation for the information about the configuration
    and statistics of the pool participating in the scheduling.
    """
    def __init__(self, pool_name, current_mem, weight, min_mem, max_mem, pool_stat):
        """
        Create a PoolInfo object to encapsulating configuration and statistics for the pool.

        :param pool_name: (str) The impala pool name.
        :param current_mem: (float) The current memory of impala pool.
        :param weight: (float) The current weight of impala pool.
        :param min_mem: (float) The minimum memory of impala pool.
        :param max_mem: (float) The maximum memory of impala pool.
        :param pool_stat: (dict) The statistics of the pool participating in the scheduling.
        """
        self.pool_name = pool_name
        self.current_mem = current_mem
        self.weight = weight
        self.min_mem = min_mem
        self.max_mem = max_mem
        self.pool_stat = pool_stat

    def __str__(self):
        return "(PoolInfo: {pool_name:%s, current_mem:%s, weight:%s, min_mem:%s, max_mem:%s, pool_stat:%s})" \
               % (self.pool_name, self.current_mem, self.weight, self.min_mem, self.max_mem, self.pool_stat)

    __repr__ = __str__


def get_pools_info(impala_pool_config, scheduler_config, pools_stat):
    """
    Get the information about the configuration and statistics of the pool participating in the
    scheduling.

    :param impala_pool_config: (dict) The configuration of impala pool.
    :param scheduler_config: (dict) The scheduler configuration in ../conf/scheduler.yml.
    :param pools_stat: (dict) The statistics of the pool participating in the scheduling.
    :return: (dict) A dict object mapping pool name to the information about the configuration and
        statistics of the pool participating in the scheduling.
    """
    pools_info = {}
    section_pool = scheduler_config[PoolSectOpts.SECT_POOL]
    for pool_name in section_pool.keys():
        current_mem = impala_pool_config.get_pool(pool_name).get_pool_mem()
        weight = impala_pool_config.get_pool(pool_name).get_pool_weight()
        min_mem = section_pool[pool_name][PoolSectOpts.OPT_MIN_MEM]
        max_mem = section_pool[pool_name][PoolSectOpts.OPT_MAX_MEM]
        pool_stat = pools_stat.get(pool_name, PoolStat())
        pool_info = PoolInfo(pool_name, current_mem, weight, min_mem, max_mem, pool_stat)
        pools_info[pool_name] = pool_info

    return pools_info
