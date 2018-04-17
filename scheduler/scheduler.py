from datetime import datetime, timedelta
import logging

from scheduler.cloudera_manager import ClouderaManager
from scheduler.constants import ScheduleSectOpts, ReportSectOpts, EmailSectOpts
from scheduler.global_utils import get_cloudera_manager_config, get_queries_info, create_schedule, send_schedule_report
from scheduler.base_schedule import get_pools_info
from scheduler.impala_pool_config import ImpalaScheduledAllocations
from scheduler.check import check_pools_allocated_mem

LOGGER = logging.getLogger(__name__)


class Scheduler(object):
    """
    The Scheduler class that provides methods for scheduling the impala pool memory according the
    statistics of fetched query information.
    """

    @classmethod
    def execute_schedule(cls, scheduler_config):
        """
        Executes impala pool memory scheduling according the configuration and the statistics
        of fetched query information.

        Execution Steps:
        1. Fetch the query information between end_time and start_time.
        2. Generate the statistic data of fetched query information.
        3. Allocate the impala pool memory based on generated statistic data.
        4. Update the allocated results to impala, actually update to cloudera manager.

        If user has set the configuration section [email], [report] and the configuration item
        [report.enable_schedule_report] is "true", the email of scheduling report will be send
        to [email.receivers] when schedule does happen.

        :param scheduler_config: (dict) The scheduler configuration in ../conf/scheduler.yml.
        """
        cloudera_manager = ClouderaManager(*get_cloudera_manager_config(scheduler_config))
        impala_config = cloudera_manager.get_impala_config()
        impala_scheduled_allocations = ImpalaScheduledAllocations(impala_config)

        end_time = datetime.now()
        section_schedule = scheduler_config[ScheduleSectOpts.SECT_SCHEDULE]
        fetch_queries_timedelta_minutes = section_schedule[ScheduleSectOpts.OPT_FETCH_QUERIES_TIMEDELTA_MINUTES]
        start_time = end_time - timedelta(minutes=fetch_queries_timedelta_minutes)

        schedule = create_schedule(section_schedule)
        queries_info = get_queries_info(cloudera_manager, section_schedule, start_time, end_time)
        pools_statistics = schedule.get_pools_stat(queries_info, start_time, end_time)

        pools_info = get_pools_info(impala_scheduled_allocations, scheduler_config, pools_statistics)
        LOGGER.info("pools information: %s", pools_info)

        pools_allocated_mem = schedule.get_pools_allocated_mem(section_schedule, pools_info)
        LOGGER.info("pools allocate memory: %s", pools_allocated_mem)

        check_pools_allocated_mem(section_schedule, pools_allocated_mem, pools_info)

        if not pools_allocated_mem:
            return

        impala_scheduled_allocations.update_pools(pools_allocated_mem)
        cloudera_manager.update_impala_config(str(impala_scheduled_allocations))
        cloudera_manager.refresh_pools()

        section_report = scheduler_config[ReportSectOpts.SECT_REPORT]
        if section_report[ReportSectOpts.OPT_ENABLE_SCHEDULE_REPORT]:
            section_email = scheduler_config[EmailSectOpts.SECT_EMAIL]
            send_schedule_report(section_email, pools_info, pools_allocated_mem, start_time, end_time)
