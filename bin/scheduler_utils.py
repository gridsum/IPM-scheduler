import logging
import json
import sys

from scheduler.check import check_required_options
from scheduler.cloudera_manager import ClouderaManager
from scheduler.global_utils import get_cloudera_manager_config
from scheduler.config_utils import ConfigUtils
from scheduler.settings import SCHEDULER_CONFIG_PATH, LOGGING_CONFIG_PATH, IMPALA_CONFIG_BACKUP_PATH
from scheduler.impala_pool_config import ImpalaScheduledAllocations

logging.config.dictConfig(ConfigUtils.read(LOGGING_CONFIG_PATH))
LOGGER = logging.getLogger(__name__)


def backup_impala_config(scheduler_config):
    """
    Backup the impala configuration before deploy the scheduler.

    :param scheduler_config: (dict) scheduler configuration in ../conf/scheduler.yml.
    """
    cloudera_manager = ClouderaManager(*get_cloudera_manager_config(scheduler_config))

    impala_config = cloudera_manager.get_impala_config(view="full")
    with open(IMPALA_CONFIG_BACKUP_PATH, "w") as f:
        json.dump(impala_config, f)

    LOGGER.info("backup impala config success")


def rollback_impala_config(scheduler_config):
    """
    Rollback the impala configuration after scheduler deploy failure.

    :param scheduler_config: (dict) scheduler configuration in ../conf/scheduler.yml.
    """
    cloudera_manager = ClouderaManager(*get_cloudera_manager_config(scheduler_config))

    with open(IMPALA_CONFIG_BACKUP_PATH, "r") as f:
        impala_config_json = json.load(f)

    impala_scheduled_allocations = ImpalaScheduledAllocations(impala_config_json)
    cloudera_manager.update_impala_config(str(impala_scheduled_allocations))
    cloudera_manager.refresh_pools()

    LOGGER.info("rollback impala config success")


if __name__ == "__main__":
    scheduler_config = ConfigUtils.read(SCHEDULER_CONFIG_PATH)
    if len(sys.argv) == 2:
        if "check" == sys.argv[1]:
            check_required_options(scheduler_config)
        elif "backup" == sys.argv[1]:
            backup_impala_config(scheduler_config)
        elif "rollback" == sys.argv[1]:
            rollback_impala_config(scheduler_config)
        else:
            sys.exit("Unknown command")
        sys.exit(0)
    else:
        print("usage: %s check|backup|rollback" % sys.argv[0])
        sys.exit(2)
