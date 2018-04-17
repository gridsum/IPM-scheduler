import logging
import math

from scheduler.constants import ScheduleSectOpts
from scheduler.base_schedule import AbstractSchedule

LOGGER = logging.getLogger(__name__)


class PrioritySchedule(AbstractSchedule):
    """
    The PrioritySchedule class that provides methods for calculating the statistic data of fetched
    query information and allocating the impala pool memory.
    The priority of pool corresponds to the weight of impala pool. If the pool is busy and its priority is high, it will give priority to free memory; vice versa.
    """

    @classmethod
    def __get_pools_moved_mem(cls, section_schedule, pools_info):
        """
        Get the memory that each pool can move according to priority.

        :param section_schedule: (dict) The schedule section of configuration in ../conf/scheduler.yml.
        :param pools_info: (dict) The information of pools that participate in the scheduling.
        :return: (dict) A dict object contains the memory status of whole pool.
        """
        busy_threshold = section_schedule[ScheduleSectOpts.OPT_BUSY_POOL_THRESHOLD_SECONDS]
        free_memory_ratio = section_schedule[ScheduleSectOpts.OPT_FREE_MEMORY_SCHEDULE_RATIO]
        memory_unit = section_schedule[ScheduleSectOpts.OPT_SCHEDULE_MEMORY_UNIT]

        temp_pools_allocated_mem = []

        for pool_info in pools_info.values():
            pool_stat = pool_info.pool_stat

            moved_mem = None

            if pool_stat.wait_secs >= busy_threshold and pool_stat.wait_mem_avg > 0:
                wait_mem = min(pool_stat.wait_mem_avg, pool_info.max_mem - pool_info.current_mem)
                moved_mem = memory_unit * math.ceil(wait_mem / memory_unit)

            free_mem = (pool_info.current_mem - max(pool_stat.used_mem_avg, pool_info.min_mem)) * free_memory_ratio
            free_mem_unit = memory_unit * math.floor(free_mem / memory_unit)
            if pool_stat.wait_secs == 0 and free_mem_unit > 0:
                moved_mem = -free_mem_unit

            if moved_mem is not None:
                temp_pool_allocated_mem = (pool_info.pool_name, moved_mem,
                                            1 if moved_mem > 0 else 0, pool_info.weight, moved_mem)
                temp_pools_allocated_mem.append(temp_pool_allocated_mem)

        temp_pools_moved_mem_sorted = sorted(temp_pools_allocated_mem, key=lambda plan: plan[2:], reverse=True)
        LOGGER.info("temp pools moved memory sorted : %s", temp_pools_moved_mem_sorted)

        pools_moved_mem = [temp_pool_allocated_mem[:2] for temp_pool_allocated_mem in temp_pools_moved_mem_sorted]
        if len(pools_moved_mem) == 0 or pools_moved_mem[-1][1] >= 0 or pools_moved_mem[0][1] <= 0:
            pools_moved_mem = []

        LOGGER.info("pools moved memory: %s", pools_moved_mem)
        return pools_moved_mem

    @classmethod
    def __allocate_mem(cls, pools_info, pools_moved_mem):
        """
        Get pools allocated memory according to priority.

        :param pools_info: (dict) The information of pools that participate in the scheduling.
        :param pools_moved_mem: (dict) The memory status of whole pool.
        :return: (dict) A dict object contains the impala memory configuration after memory allocation.
        """
        pools_allocated_mem = {}
        start_index = 0
        end_index = len(pools_moved_mem) - 1
        while start_index < end_index:
            queued_pool, queued_mem = pools_moved_mem[start_index]
            queued_current_mem = pools_allocated_mem.get(queued_pool, pools_info.get(queued_pool).current_mem)
            free_pool, free_mem = pools_moved_mem[end_index]
            free_current_mem = pools_allocated_mem.get(free_pool, pools_info.get(free_pool).current_mem)

            if queued_mem < 0:
                start_index += 1
                continue
            if free_mem > 0:
                end_index -= 1
                continue

            difference = queued_mem + free_mem
            if difference > 0:
                pools_allocated_mem[queued_pool] = queued_current_mem - free_mem
                pools_allocated_mem[free_pool] = free_current_mem + free_mem
                queued_mem = difference
                pools_moved_mem[start_index] = (queued_pool, queued_mem)
                end_index -= 1
            else:
                pools_allocated_mem[queued_pool] = queued_current_mem + queued_mem
                pools_allocated_mem[free_pool] = free_current_mem - queued_mem
                free_mem = difference
                pools_moved_mem[end_index] = (free_pool, free_mem)
                start_index += 1
        return pools_allocated_mem

    @classmethod
    def get_pools_allocated_mem(cls, section_schedule, pools_info):
        """
        Get the allocated memory of whole pool.

        :param section_schedule: (dict) The schedule section of configuration in ../conf/scheduler.yml.
        :param pools_info: (dict) The information of pools that participate in the scheduling.
        :return: (dict) A dict object contains allocated memory of whole pool.
        """
        pools_moved_mem = PrioritySchedule.__get_pools_moved_mem(section_schedule, pools_info)
        return PrioritySchedule.__allocate_mem(pools_info, pools_moved_mem)

