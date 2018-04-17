IMPALA_SCHEDULED_ALLOCATIONS = "impala_scheduled_allocations"
QUERY_DATA_SAVE_PATH_PREFIX = "data-"
SCHEDULER_HOME = "SCHEDULER_HOME"


class NativeQueryInfoColumn(object):
    """
    The wrapper class contains the items of native query information.
    """
    QUERIES = "queries"
    QUERY_ID = "queryId"
    START_TIME = "startTime"
    DURATION_MILLIS = "durationMillis"
    ATTRIBUTES = "attributes"
    POOL = "pool"
    ADMISSION_WAIT = "admission_wait"
    DETAILS = "details"


class FormativeQueryInfoColumn(object):
    """
    The wrapper class contains the items of formatted query information.
    """
    QUERY_ID = "query_id"
    START_TIME = "start_time"
    DURATION_MILLIS = "duration_millis"
    POOL = "pool"
    ADMISSION_WAIT = "admission_wait"
    MEM_LIMIT = "mem_limit"
    MAX_HOST = "max_host"


class ClouderaManagerSectOpts(object):
    """
    The wrapper class contains the configuration items of cloudera manager section.
    """
    SECT_CLOUDERA_MANAGER = "cloudera_manager"
    OPT_CLUSTER_NAME = "cluster_name"
    OPT_SERVER_URL = "server_url"
    OPT_API_VERSION = "api_version"
    OPT_USERNAME = "username"
    OPT_PASSWORD = "password"


class ScheduleSectOpts(object):
    """
    The wrapper class contains the configuration items of schedule section.
    """
    SECT_SCHEDULE = "schedule"
    OPT_SCHEDULE_AVAILABLE_IMPALAD_THRESHOLD = "schedule_available_impalad_threshold"
    OPT_SCHEDULE_INTERVAL_MINUTES = "schedule_interval_minutes"
    OPT_FREE_MEMORY_SCHEDULE_RATIO = "free_memory_schedule_ratio"
    OPT_SCHEDULE_MEMORY_UNIT = "schedule_memory_unit"
    OPT_BUSY_POOL_THRESHOLD_SECONDS = "busy_pool_threshold_seconds"
    OPT_SCHEDULE_MODULE_NAME = "schedule_module_name"
    OPT_SCHEDULE_PY_NAME = "schedule_py_name"
    OPT_SCHEDULE_CLASS_NAME = "schedule_class_name"
    OPT_FETCH_QUERIES_TIMEDELTA_MINUTES = "fetch_queries_timedelta_minutes"
    OPT_FETCH_QUERIES_FILTER = "fetch_queries_filter"
    OPT_ENABLE_FETCH_QUERIES_FILE = "enable_fetch_queries_file"


class PoolSectOpts(object):
    """
    The wrapper class contains the configuration items of pool section.
    """
    SECT_POOL = "pool"
    OPT_MIN_MEM = "min_mem"
    OPT_MAX_MEM = "max_mem"


class EmailSectOpts(object):
    """
    The wrapper class contains the configuration items of email section.
    """
    SECT_EMAIL = "email"
    OPT_SERVER = "server"
    OPT_USERNAME = "username"
    OPT_PASSWORD = "password"
    OPT_RECEIVERS = "receivers"


class ReportSectOpts(object):
    """
    The wrapper class contains the configuration items of report section.
    """
    SECT_REPORT = "report"
    OPT_ENABLE_SCHEDULE_REPORT = "enable_schedule_report"
    OPT_ENABLE_MONITOR_REPORT = "enable_monitor_report"


class ReportColumn(object):
    """
    The wrapper class contains the columns of report.
    """
    RESOURCE_POOL = "ResourcePool"
    MEM_BEFORE_SCHEDULE = "MemBeforeSchedule"
    MEM_AFTER_SCHEDULE = "MemAfterSchedule"
    MEM_MOVED = "MemMoved"
    MEM_USED = "MemUsed"
    MEM_LACK = "MemLacked"
    QUERY_NUMBER = "QueryNumber"
    WORK_TIME = "WorkTime"
    QUEUED_TIME = "QueuedTime"
    WEIGHT = "Weight"
    MIN_MEM = "MinMem"
    MAX_MEM = "MaxMem"
