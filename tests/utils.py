import json

from scheduler.config_utils import ConfigUtils
from scheduler.impala_pool_config import ImpalaScheduledAllocations
from scheduler.constants import ScheduleSectOpts
from scheduler.base_schedule import get_pools_info


def get_impala_pool_config():
    with open("./resources/impala_config_test.json", "r") as f:
        content = f.read()
    impala_config_json = json.loads(content)
    impala_scheduled_allocations = ImpalaScheduledAllocations(impala_config_json)
    return impala_scheduled_allocations


def get_scheduler_config():
    return ConfigUtils.read("./resources/scheduler_test.yml")


def get_test_pools_info(pools_stat):
    impala_pool_config = get_impala_pool_config()
    scheduler_config = get_scheduler_config()
    pools_info = get_pools_info(impala_pool_config, scheduler_config, pools_stat)
    return pools_info


def get_test_pools_allocated_mem(schedule, pools_stat):
    scheduler_config = get_scheduler_config()
    schedule_config = scheduler_config[ScheduleSectOpts.SECT_SCHEDULE]
    pools_info = get_test_pools_info(pools_stat)
    pools_allocated_mem = schedule.get_pools_allocated_mem(schedule_config, pools_info)

    return pools_allocated_mem
