import logging

from scheduler.constants import ClouderaManagerSectOpts, ScheduleSectOpts, PoolSectOpts, EmailSectOpts, ReportSectOpts
from scheduler.cloudera_manager import ClouderaManager
from scheduler.global_utils import get_cloudera_manager_config
from scheduler.impala_pool_config import ImpalaScheduledAllocations

REQUIRED_CONFIG_SECTIONS = [ClouderaManagerSectOpts.SECT_CLOUDERA_MANAGER,
                            ScheduleSectOpts.SECT_SCHEDULE,
                            PoolSectOpts.SECT_POOL,
                            EmailSectOpts.SECT_EMAIL,
                            ReportSectOpts.SECT_REPORT]

REQUIRED_CLOUDERA_MANAGER_OPTIONS = [ClouderaManagerSectOpts.OPT_CLUSTER_NAME,
                                     ClouderaManagerSectOpts.OPT_SERVER_URL,
                                     ClouderaManagerSectOpts.OPT_API_VERSION,
                                     ClouderaManagerSectOpts.OPT_USERNAME,
                                     ClouderaManagerSectOpts.OPT_PASSWORD]

REQUIRED_SCHEDULE_OPTIONS = [ScheduleSectOpts.OPT_SCHEDULE_INTERVAL_MINUTES,
                             ScheduleSectOpts.OPT_SCHEDULE_MEMORY_UNIT,
                             ScheduleSectOpts.OPT_FREE_MEMORY_SCHEDULE_RATIO,
                             ScheduleSectOpts.OPT_BUSY_POOL_THRESHOLD_SECONDS,
                             ScheduleSectOpts.OPT_FETCH_QUERIES_TIMEDELTA_MINUTES]

REQUIRED_EMAIL_OPTIONS = [EmailSectOpts.OPT_SERVER,
                          EmailSectOpts.OPT_USERNAME,
                          EmailSectOpts.OPT_PASSWORD,
                          EmailSectOpts.OPT_RECEIVERS]

REQUIRED_REPORT_OPTIONS = [ReportSectOpts.OPT_ENABLE_SCHEDULE_REPORT,
                           ReportSectOpts.OPT_ENABLE_MONITOR_REPORT]

LOGGER = logging.getLogger(__name__)

ITEMS = "items"
TYPE = "type"
TYPE_IMPALAD = "IMPALAD"
TYPE_STATE_STORE = "STATESTORE"
HEALTH_SUMMARY = "healthSummary"
HEALTH_SUMMARY_VALUE = "GOOD"


def check_required_sections(scheduler_config):
    """
    Check the sections that must be configured.

    :param scheduler_config: (dict) The scheduler configuration in ../conf/scheduler.yml.
    """
    for section in REQUIRED_CONFIG_SECTIONS:
        if section not in scheduler_config:
            LOGGER.error("section [%s] is required.", section)
            raise KeyError("section [{}] is required.".format(section))


def check_required_options(scheduler_config):
    """
    Check the options that must be configured.

    :param scheduler_config: (dict) The scheduler configuration in ../conf/scheduler.yml.
    """
    check_cloudera_manager_options(scheduler_config)

    check_schedule_options(scheduler_config)

    cloudera_manager = ClouderaManager(*get_cloudera_manager_config(scheduler_config))
    impala_config_json = cloudera_manager.get_impala_config()
    impala_scheduled_allocations = ImpalaScheduledAllocations(impala_config_json)
    check_pool_options(impala_scheduled_allocations, scheduler_config)

    check_report_options(scheduler_config)


def check_cloudera_manager_options(scheduler_config):
    """
    Check the cloudera manager options that must be configured.

    :param scheduler_config: (dict) The scheduler configuration in ../conf/scheduler.yml.
    """
    section_cloudera_manager = scheduler_config[ClouderaManagerSectOpts.SECT_CLOUDERA_MANAGER]

    for option in REQUIRED_CLOUDERA_MANAGER_OPTIONS:
        # check required key
        if option not in section_cloudera_manager:
            LOGGER.error("option [%s] is required in section_cloudera_manager.", option)
            raise KeyError("option [{}] is required in section_cloudera_manager.".format(option))

        # check required value
        if not section_cloudera_manager[option]:
            LOGGER.error("option [%s: %s] is not allowed.", option, section_cloudera_manager[option])
            raise ValueError("option [{}: {}] is not allowed.".format(option, section_cloudera_manager[option]))


def check_schedule_options(scheduler_config):
    """
    Check the schedule options that must be configured.

    :param scheduler_config: (dict) The scheduler configuration in ../conf/scheduler.yml.
    """
    section_schedule = scheduler_config[ScheduleSectOpts.SECT_SCHEDULE]

    for option in REQUIRED_SCHEDULE_OPTIONS:
        # check required option
        if option not in section_schedule:
            LOGGER.error("option [%s] is required in section_schedule.", option)
            raise KeyError("option [{}] is required in section_schedule.".format(option))

        # check required value
        if not section_schedule[option]:
            LOGGER.error("option [%s: %s] is not allowed.", option, section_schedule[option])
            raise ValueError("option [{}: {}] is not allowed.".format(option, section_schedule[option]))
        if ScheduleSectOpts.OPT_FREE_MEMORY_SCHEDULE_RATIO == option and not (0 < section_schedule[option] <= 1.0):
            LOGGER.error("option [%s: %s] is not allowed, it must be valued in (0, 1.0].",
                         option, section_schedule[option])
            raise ValueError("option [{}: {}] is not allowed, it must be valued in (0, 1.0]."
                             .format(option, section_schedule[option]))


def check_pool_options(impala_scheduled_allocations, scheduler_config):
    """
    Check the pool options that must be configured.

    :param impala_scheduled_allocations: (dict) The impala configuration fetched from cloudera manager.
    :param scheduler_config: (dict) The scheduler configuration in ../conf/scheduler.yml.
    """
    section_pool = scheduler_config[PoolSectOpts.SECT_POOL]
    pool_names = impala_scheduled_allocations.get_pool_names()

    for pool_name, pool_value in section_pool.items():
        # check pool name
        if pool_name not in pool_names:
            LOGGER.error("option [%s: %s] is not allowed, it must be valued in %s.",
                         pool_name, section_pool[pool_name], pool_names)
            raise KeyError("option [{}: {}] is not allowed, it must be valued in {}"
                           .format(pool_name, section_pool[pool_name], pool_names))

        # check pool value
        if not (0 < pool_value[PoolSectOpts.OPT_MIN_MEM] <=
                impala_scheduled_allocations.get_pool(pool_name).get_pool_mem() <=
                pool_value[PoolSectOpts.OPT_MAX_MEM]):
            LOGGER.error("option [%s: %s] is not allowed, it must be valued in 0 < %s <= %s <= %s",
                         pool_name, section_pool[pool_name], pool_value[PoolSectOpts.OPT_MIN_MEM],
                         impala_scheduled_allocations.get_pool(pool_name).get_pool_mem(),
                         pool_value[PoolSectOpts.OPT_MAX_MEM])
            raise ValueError("option [{}: {}] is not allowed, it must be valued in 0 < {} <= {} <= {}"
                             .format(pool_name, section_pool[pool_name], pool_value[PoolSectOpts.OPT_MIN_MEM],
                                     impala_scheduled_allocations.get_pool(pool_name).get_pool_mem(),
                                     pool_value[PoolSectOpts.OPT_MAX_MEM]))


def check_email_options(scheduler_config):
    """
    Check the email options that must be configured.

    :param scheduler_config: (dict) The scheduler configuration in ../conf/scheduler.yml.
    """
    section_email = scheduler_config[EmailSectOpts.SECT_EMAIL]

    for option in REQUIRED_EMAIL_OPTIONS:
        # check required key
        if option not in section_email:
            LOGGER.error("option [%s] is required in section_email.", option)
            raise KeyError("option [{}] is required in section_email.".format(option))

        # check required value
        if not section_email[option]:
            LOGGER.error("option [%s: %s] is not allowed.", option, section_email[option])
            raise ValueError("option [{}: {}] is not allowed.".format(option, section_email[option]))


def check_report_options(scheduler_config):
    """
    Check the report options that must be configured.

    :param scheduler_config: (dict) The scheduler configuration in ../conf/scheduler.yml.
    """
    section_report = scheduler_config[ReportSectOpts.SECT_REPORT]

    is_depends_email = False
    for option in section_report:
        is_depends_email |= section_report[option]

    if is_depends_email:
        check_email_options(scheduler_config)


def check_impala_health(scheduler_config):
    """
    Check the health of impala cluster.

    :param scheduler_config: (dict) The scheduler configuration in ../conf/scheduler.yml.
    :return: (bool) a bool object represent the health status of impala cluster.
    """
    cloudera_manager = ClouderaManager(*get_cloudera_manager_config(scheduler_config))
    section_schedule = scheduler_config.get(ScheduleSectOpts.SECT_SCHEDULE)
    schedule_available_impalad_threshold = section_schedule[ScheduleSectOpts.OPT_SCHEDULE_AVAILABLE_IMPALAD_THRESHOLD]
    health_imaplad_count = 0
    health_state_store_status = False
    for role in cloudera_manager.get_roles().get(ITEMS):
        temp_type = role.get(TYPE)
        heath_summary = role.get(HEALTH_SUMMARY)
        if temp_type == TYPE_IMPALAD and heath_summary == HEALTH_SUMMARY_VALUE:
            health_imaplad_count += 1
        elif temp_type == TYPE_STATE_STORE and heath_summary == HEALTH_SUMMARY_VALUE:
            health_state_store_status = True

    if health_imaplad_count <= schedule_available_impalad_threshold:
        LOGGER.warning("min impalad service number is %d, current number is %d" %
                        (schedule_available_impalad_threshold, health_imaplad_count))
        return False

    if not health_state_store_status:
        LOGGER.warning("impala state store service is not good")
        return False

    LOGGER.info("current %d impala impalad and state store are healthy" % health_imaplad_count)
    return True


def check_pools_allocated_mem(section_schedule, pools_allocated_mem, pools_info):
    """
    Check the allocated memory of the pool participating in the scheduling.

    :param section_schedule: (dict) The schedule section of configuration in ../conf/scheduler.yml.
    :param pools_allocated_mem: (dict) The allocated memory of the pool participating in the scheduling.
    :param pools_info: (dict) The information about the configuration and statistics of the pool
        participating in the scheduling.
    """
    for pool, allocated_mem in pools_allocated_mem.items():
        schedule_module_name = section_schedule[ScheduleSectOpts.OPT_SCHEDULE_CLASS_NAME]
        pool_info = pools_info.get(pool)
        if not pool_info:
            raise Exception("pool(%s) is not in scheduler conf, maybe schedule class(%s) has bug"
                            % (pool, schedule_module_name))
        if not pool_info.min_mem <= allocated_mem <= pool_info.max_mem:
            raise Exception("the allocated memory value(%sMB) of pool(%s) is valid, min memory is %sMB, "
                            "max memory is %sMB, maybe schedule class(%s) has bug"
                            % (allocated_mem, pool, pool_info.min_mem, pool_info.max_mem, schedule_module_name))

