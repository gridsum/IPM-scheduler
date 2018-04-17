import os

from scheduler.constants import SCHEDULER_HOME

scheduler_home = os.environ[SCHEDULER_HOME]

SCHEDULER_CONFIG_PATH = "%s/conf/scheduler.yml" % scheduler_home
LOGGING_CONFIG_PATH = "%s/conf/logging.yml" % scheduler_home
LOG_FILE_PATH = "%s/logs" % scheduler_home
PID_FILE_PATH = "%s/logs/.daemon.pid" % scheduler_home
IMPALA_CONFIG_BACKUP_PATH = "%s/resources/impala_config_backup.json" % scheduler_home
REPORT_TEMPLATE_PATH = "%s/resources/schedule_report_templet.html" % scheduler_home
