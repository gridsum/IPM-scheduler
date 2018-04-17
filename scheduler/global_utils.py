from datetime import datetime
from email.mime.text import MIMEText
from email.header import Header
from tornado.template import Template
import smtplib
import math
import logging
import traceback
import os
import time
import pandas as pd

from scheduler.constants import ClouderaManagerSectOpts, EmailSectOpts,\
    ReportColumn, ScheduleSectOpts, QUERY_DATA_SAVE_PATH_PREFIX
from scheduler.settings import REPORT_TEMPLATE_PATH, LOG_FILE_PATH
from scheduler.base_schedule import ScheduleInterface

LOGGER = logging.getLogger(__name__)


def send_email(section_email, message):
    """
    Send email.

    :param section_email: (dict) The configuration of email section.
    :param message: (str) The message to be send.
    """
    server = section_email[EmailSectOpts.OPT_SERVER]
    username = section_email[EmailSectOpts.OPT_USERNAME]
    password = section_email[EmailSectOpts.OPT_PASSWORD]
    receivers = section_email[EmailSectOpts.OPT_RECEIVERS].split(",")

    smtp_bbj = smtplib.SMTP(server)
    smtp_bbj.login(user=username, password=password)
    smtp_bbj.sendmail(username, receivers, message.as_string())
    smtp_bbj.quit()


def generate_schedule_report_data(pools_info, pools_allocated_mem):
    """
    Generate the schedule report data.

    :param pools_info: (dict) The information about the configuration and statistics of the pool participating
        in the scheduling.
    :param pools_allocated_mem: (dict) The allocated memory of the pool participating in the scheduling.
    :return: (DataFrame) A DataFrame object of report data.
    """
    columns = [ReportColumn.RESOURCE_POOL,
               ReportColumn.MEM_BEFORE_SCHEDULE,
               ReportColumn.MEM_AFTER_SCHEDULE,
               ReportColumn.MEM_MOVED,
               ReportColumn.MEM_USED,
               ReportColumn.MEM_LACK,
               ReportColumn.QUERY_NUMBER,
               ReportColumn.WORK_TIME,
               ReportColumn.QUEUED_TIME,
               ReportColumn.WEIGHT,
               ReportColumn.MIN_MEM,
               ReportColumn.MAX_MEM]

    data = [[pool_info.pool_name,
             int(convert_mem_unit(pool_info.current_mem)),
             int(convert_mem_unit(pools_allocated_mem.get(pool_info.pool_name, pool_info.current_mem))),
             int(convert_mem_unit(pools_allocated_mem.get(pool_info.pool_name, pool_info.current_mem)
                                  - pool_info.current_mem)),
             int(convert_mem_unit(pool_info.pool_stat.used_mem_avg)) \
                 if int(convert_mem_unit(pool_info.pool_stat.wait_mem_avg)) == 0 \
                 else int(convert_mem_unit(pool_info.current_mem)),
             int(convert_mem_unit(pool_info.pool_stat.wait_mem_avg)),
             pool_info.pool_stat.query_total,
             int(pool_info.pool_stat.run_secs),
             int(pool_info.pool_stat.wait_secs),
             pool_info.weight,
             int(convert_mem_unit(pool_info.min_mem)),
             int(convert_mem_unit(pool_info.max_mem))]
            for pool_info in list(pools_info.values())]

    return pd.DataFrame(data, columns=columns)


def send_schedule_report(section_email, pools_info, pools_allocated_mem, start_time, end_time):
    """
    Send the schedule report.

    Steps:
    1. Fill the report data in a html template.
    2. Transform the html template to string message.
    3. Send email using the above message.

    :param section_email: (dict) The email section of configuration in ../conf/scheduler.yml.
    :param pools_info: (dict) The information about the configuration and statistics of the pool
        participating in the scheduling.
    :param pools_allocated_mem: (dict) The allocated memory of the pool participating in the scheduling.
    :param start_time: (datetime) The start time to fetching query information.
    :param end_time: (datetime) The end time to fetching query information.
    """
    report_data = generate_schedule_report_data(pools_info, pools_allocated_mem)
    with open(REPORT_TEMPLATE_PATH, "r") as f:
        html_template = f.read()
    hd = Template(html_template)
    text = hd.generate(df=report_data, schedule_start_time=start_time, schedule_end_time=end_time).decode("utf8")
    message = MIMEText(text, _subtype="html")
    message["Subject"] = Header("{} ~ {} impala memory schedule report".format(start_time, end_time), "utf-8")
    send_email(section_email, message)


def send_monitor_report(section_email, text):
    """
    Send the monitor report.

    :param section_email: (dict) The email section of configuration in ../conf/scheduler.yml.
    :param text: (str) The text to be send.
    """
    message = MIMEText(text, _subtype="html")
    message["Subject"] = Header("scheduler daemon down", "utf-8")
    send_email(section_email, message)


def retry(func, max_try_times=2):
    """
    Retry the http request. Exception will be raised when execute times exceed the max try times.

    By default, the max try times is 2, it means execute a normal http request and retry one time
    when normal http request failed.

    :param func: (str) The retry function name.
    :param max_try_times: (int) The max try times. By default, it's value is 2.
    :return: Reference the result of function to be executed.
    """
    def wrapper(*args, **kwargs):
        if max_try_times < 0:
            raise ValueError("max_try_times: {} must be a natural number(>=0)".format(max_try_times))

        for i in range(max_try_times):
            try:
                return func(*args, **kwargs)
            except Exception:
                LOGGER.info("try %d times to call fun:%s fail", i + 1, func)
        else:
            raise Exception("call fun:{} failed, caused by: {}".format(func, traceback.format_exc()))
    return wrapper


def spend_time(func):
    """
    Execute function and record the spent time into the log file.

    :param func: (str) The function name.
    :return: Reference the result of function to be executed.
    """
    def wrapper(*args, **kwargs):
        api_start_time = datetime.now()

        response = func(*args, **kwargs)

        api_end_time = datetime.now()
        spent_time = api_end_time - api_start_time
        LOGGER.debug("call api %s spent %ds", func.__name__, spent_time.seconds)
        return response
    return wrapper


UNIT_LIST = ["PB", "TB", "GB", "MB", "KB", "B"]


def convert_mem_unit(mem_value, from_unit="MB", to_unit="GB"):
    """
    Convert memory unit.

    By default, from_unit is "MB" and to_unit is "GB". See the UNIT_LIST for their value range.

    :param mem_value: (float) The memory value to be converted.
    :param from_unit: (str) The memory unit before conversion.
    :param to_unit: (str) The memory unit after conversion.
    :return: (float) The memory value after conversion.
    """
    from_unit_index = UNIT_LIST.index(to_unit.upper())
    if from_unit_index < 0:
        raise Exception("unknown from_unit: %s" % from_unit)
    to_unit_index = UNIT_LIST.index(from_unit.upper())
    if to_unit_index < 0:
        raise Exception("unknown to_unit: %s" % to_unit)
    return mem_value * math.pow(1024, (from_unit_index - to_unit_index))


def get_cloudera_manager_config(scheduler_config):
    """
    Get the configuration of cloudera manager.

    :param scheduler_config: (dict) The scheduler configuration in ../conf/scheduler.yml.
    :return: (tuple) A tuple object contains whole the configuration of cloudera manager.
    """
    section_cloudera_manager = scheduler_config.get(ClouderaManagerSectOpts.SECT_CLOUDERA_MANAGER)
    server_url = section_cloudera_manager[ClouderaManagerSectOpts.OPT_SERVER_URL]
    api_version = section_cloudera_manager[ClouderaManagerSectOpts.OPT_API_VERSION]
    cluster_name = section_cloudera_manager[ClouderaManagerSectOpts.OPT_CLUSTER_NAME]
    username = section_cloudera_manager[ClouderaManagerSectOpts.OPT_USERNAME]
    password = section_cloudera_manager[ClouderaManagerSectOpts.OPT_PASSWORD]

    return server_url, api_version, cluster_name, username, password


def clean_expired_files(clean_directory, file_name_prefix="", expired_days=1):
    """
    Clean the expired files.

    By default, expire days is 1.

    :param clean_directory: (str) The directory of files to be cleaned.
    :param file_name_prefix: (str) The name prefix of files to be cleaned.
    :param expired_days: (int) The expired days.
    """
    now = datetime.now()
    files = os.listdir(clean_directory)
    for file in files:
        if file_name_prefix and not file.startswith(file_name_prefix):
            continue

        update_timestamp = time.localtime(os.stat(os.path.join(clean_directory + "/" + file)).st_mtime)
        time_to_now = now - datetime.fromtimestamp(time.mktime(update_timestamp))
        if time_to_now.days > expired_days:
            os.remove(os.path.join(clean_directory + "/" + file))


def get_queries_info(cloudera_manager, section_schedule, start_time, end_time):
    """
    Get total query information.

    If user has set the configuration item [schedule.enable_fetch_queries_file] to "true", the
    fetched query information will be save to local file with name format: data-xxx.txt.

    :param cloudera_manager: (ClouderManager) The cloudera manager object.
    :param section_schedule: (dict) The schedule section of configuration in ../conf/scheduler.yml.
    :param start_time: (datetime) The start time to fetching query information.
    :param end_time: (datetime) The end time to fetching query information.
    :return: (DataFrame) A DataFrame object of total fetched query information.
    """
    query_data_save_enable = section_schedule[ScheduleSectOpts.OPT_ENABLE_FETCH_QUERIES_FILE]
    filter_str = section_schedule[ScheduleSectOpts.OPT_FETCH_QUERIES_FILTER]
    queries_info = cloudera_manager.fetch_impala_query_info(start_time, end_time, filter_str)

    if queries_info is None:
        LOGGER.info("queries info between: %s ~ %s size is 0", str(start_time), str(end_time))
    elif query_data_save_enable:
        LOGGER.info("queries info between: %s ~ %s size is %d", str(start_time), str(end_time), queries_info.shape[0])
        query_data_save_path = "%s/%s%s.csv" % (LOG_FILE_PATH, QUERY_DATA_SAVE_PATH_PREFIX, end_time)
        queries_info.to_csv(path_or_buf=query_data_save_path, encoding="utf-8", index=False)
    return queries_info


def create_schedule(section_schedule):
    """
    Create a object of schedule class.

    By default, current project use the PrioritySchedule class to execute schedule. if user don't
    want to implement a schedule strategy by yourself, keep the follow default configuration items:
    [schedule.schedule_module_name], [schedule.schedule_py_name] and [schedule.schedule_class_name].
    On the contrary, see module base_schedule.

    :param section_schedule: (dict) The schedule section of configuration in ../conf/scheduler.yml.
    :return: (AbstractSchedule) A AbstractSchedule object is used to execute schedule steps.
    """
    schedule_module_name = section_schedule[ScheduleSectOpts.OPT_SCHEDULE_MODULE_NAME]
    schedule_py_name = section_schedule[ScheduleSectOpts.OPT_SCHEDULE_PY_NAME]
    schedule_class_name = section_schedule[ScheduleSectOpts.OPT_SCHEDULE_CLASS_NAME]

    schedule_module = __import__("%s.%s" % (schedule_module_name, schedule_py_name))
    schedule_py = getattr(schedule_module, schedule_py_name)
    schedule_class = getattr(schedule_py, schedule_class_name)
    if not issubclass(schedule_class, ScheduleInterface):
        raise Exception("illegal schedule class(%s), please extend from ScheduleInterface or AbstractSchedule"
                        % schedule_class)

    return schedule_class
