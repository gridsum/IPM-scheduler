import sys
import logging.config
import os
import signal
import traceback
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

from scheduler.config_utils import ConfigUtils
from scheduler.check import check_required_sections, check_required_options, check_impala_health
from scheduler.scheduler import Scheduler
from scheduler.constants import ScheduleSectOpts, ReportSectOpts, EmailSectOpts, QUERY_DATA_SAVE_PATH_PREFIX
from scheduler.settings import SCHEDULER_CONFIG_PATH, LOGGING_CONFIG_PATH, PID_FILE_PATH, LOG_FILE_PATH
from scheduler.global_utils import send_monitor_report, clean_expired_files

logging.config.dictConfig(ConfigUtils.read(LOGGING_CONFIG_PATH))
LOGGER = logging.getLogger(__name__)


def memory_scheduling_job(scheduler_config):
    """
    A job for scheduling impala memory.
    Once check exception occurred, scheduler will be stop and whether to send an email based
    on current report configuration item [enable_monitor_report].

    Firstly, check scheduler configuration for compliance.
    Secondly, check health of impala cluster.
    Thirdly, schedule impala memory according the fetched query information.
    Fourthly, clean the fetched query information that have expired.

    :param scheduler_config: (dict) scheduler configuration in ../conf/scheduler.yml.
    """
    try:
        check_required_sections(scheduler_config)

        check_required_options(scheduler_config)

        if not check_impala_health(scheduler_config):
            LOGGER.warning("skip current scheduling, because of impala unhealthy.")
            return

        Scheduler.execute_schedule(scheduler_config)

        clean_expired_files(LOG_FILE_PATH, QUERY_DATA_SAVE_PATH_PREFIX)
    except Exception:
        LOGGER.error("fail to execute memory schedule job.\n %s" % traceback.format_exc())
        try:
            section_report = scheduler_config[ReportSectOpts.SECT_REPORT]
            if section_report[ReportSectOpts.OPT_ENABLE_SCHEDULE_REPORT]:
                section_email = scheduler_config[EmailSectOpts.SECT_EMAIL]
                send_monitor_report(section_email, traceback.format_exc())
        except Exception:
            LOGGER.error("fail to send monitor report.\n %s" % traceback.format_exc())

        # TBD: which exceptions cause stop
        stop()


def start():
    """
    Start the scheduler in the foreground. The scheduler will execute immediately and every
    [schedule_interval_minutes] minutes thereafter.
    """
    if os.path.exists(PID_FILE_PATH):
        with open(PID_FILE_PATH, "r") as f_pid:
            pid = f_pid.read().strip()
        if pid:
            LOGGER.warning("pid file %s is not empty. Daemon is running?", PID_FILE_PATH)
            return

    pid = str(os.getpid())
    with open(PID_FILE_PATH, "w") as f_pid:
        f_pid.write("%s\n" % pid)

    scheduler_config = ConfigUtils.read(SCHEDULER_CONFIG_PATH)
    minutes = scheduler_config[ScheduleSectOpts.SECT_SCHEDULE][ScheduleSectOpts.OPT_SCHEDULE_INTERVAL_MINUTES]

    scheduler = BlockingScheduler()
    scheduler.add_job(memory_scheduling_job, trigger='interval', args=[scheduler_config],
                      minutes=minutes, next_run_time=datetime.now())
    scheduler.start()


def start_with_daemon():
    """
    Start the scheduler in the background. The scheduler will execute immediately and every
    [schedule_interval_minutes] minutes thereafter.
    """
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except Exception:
        LOGGER.error(traceback.format_exc())
        sys.exit(1)

    os.setsid()
    os.umask(0)

    start()


def stop():
    """
    Stop the scheduler.
    """
    if not os.path.exists(PID_FILE_PATH):
        LOGGER.error("pid file %s does not exist. Daemon not running?", PID_FILE_PATH)
        return

    try:
        with open(PID_FILE_PATH, "r") as f_pid:
            pid = f_pid.read().strip()
        if not pid:
            LOGGER.error("pid file %s is empty. Daemon not running?", PID_FILE_PATH)
            return
        if os.path.exists(PID_FILE_PATH):
            os.remove(PID_FILE_PATH)

        os.kill(int(pid), signal.SIGKILL)
    except Exception:
        LOGGER.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":

    if len(sys.argv) == 2:
        if "start" == sys.argv[1]:
            start_with_daemon()
        elif "stop" == sys.argv[1]:
            stop()
        elif "restart" == sys.argv[1]:
            stop()
            start_with_daemon()
        else:
            sys.exit("Unknown command: %s" % sys.argv[1:])
        print("%s successfully" % sys.argv[1])
    else:
        print("usage: %s start|stop|restart" % sys.argv[0])
        sys.exit(1)

    sys.exit(0)
