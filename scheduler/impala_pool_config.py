import logging
import json

from scheduler.constants import IMPALA_SCHEDULED_ALLOCATIONS

ITEMS = "items"
VALUE = "value"
POOLS = "queues"
NAME = "name"
SCHEDULABLE_PROPERTIES_LIST = "schedulablePropertiesList"
IMPALA_MAX_MEMORY = "impalaMaxMemory"
WEIGHT = "weight"
IMPALA_POOL_TIMEOUT = "impalaQueueTimeout"

ROOT_PARENT_POOL_NAME = ""
LEAF_SUB_POOL_NAME = ""
DOT_DELIMITER = "."

LOGGER = logging.getLogger(__name__)


class ImpalaPool(object):
    """
    The ImpalaPool class that provides methods for get and update configuration of impala pool.

    :param pool_name: (str) The impala pool name.
    :param pool: (dict) The current pool containing related configuration.
    """

    def __init__(self, pool_name, pool):
        self.__pool_name = pool_name
        self.__pool = pool

        self.__schedulable_properties_list = self.__pool[SCHEDULABLE_PROPERTIES_LIST][0]

    def __repr__(self):
        return str(self.__pool)

    def __str__(self):
        return self.__repr__()

    def get_pool_mem(self):
        """
        Get the memory item of current pool. unit: MB

        :return: (float) A float value represent impala pool memory.
        """
        return self.__schedulable_properties_list[IMPALA_MAX_MEMORY]

    def update_pool_mem(self, memory):
        """
        Update the memory item of current pool. unit: MB

        :param memory: (float) The memory value to be updated.
        """
        self.__schedulable_properties_list[IMPALA_MAX_MEMORY] = memory

    def get_pool_weight(self):
        """
        Get the weight item of current pool.

        :return: (float) A float value represent impala pool weight.
        """
        return self.__schedulable_properties_list[WEIGHT]


class ImpalaScheduledAllocations(object):
    """
    The ImpalaScheduledAllocations class provides methods to get and update impala scheduled allocations
    configuration item, which also one of whole impala configuration items.

    :param primary_impala_config: (dict) The primary impala configuration that fetched from cloudera manager.
        For example:
            {
                "items": [{
                        "relatedName": "",
                        "displayName": "EnableDynamicResourcePools",
                        "name": "admission_control_enabled",
                        "default": "true",
                        "required": False,
                        "value": "true",
                        "validationState": "OK",
                    },
                    {
                        "relatedName": "",
                        "displayName": "EnableImpalaAdmissionControl",
                        "name": "all_admission_control_enabled",
                        "default": "true",
                        "required": False,
                        "value": "true",
                        "validationState": "OK",
                    }]
            }
    """

    def __init__(self, primary_impala_config):
        """
        Create a ImpalaScheduledAllocations object.

        :param primary_impala_config: (dict) The primary impala configuration that fetched from cloudera
            manager.
        """
        impala_config_dict = {item[NAME]: item for item in primary_impala_config[ITEMS]}
        self.__allocations_value = json.loads(impala_config_dict[IMPALA_SCHEDULED_ALLOCATIONS][VALUE])

    def __str__(self):
        value = json.dumps(self.__allocations_value)
        formatted_impala_scheduled_allocations = {ITEMS: [{NAME: IMPALA_SCHEDULED_ALLOCATIONS, VALUE: value}]}
        return json.dumps(formatted_impala_scheduled_allocations)

    def __get_pool_names(self, pools, parent_pool_name, pool_names):
        """
        Get the whole pool names and store in list pool_names.

        :param pools: (dict) The whole impala pool configuration.
        :param parent_pool_name: (str) The parent pool name.
        :param pool_names: (list) The pool name list.
        """
        for pool in pools:
            current_pool_name = parent_pool_name + DOT_DELIMITER + pool[NAME] if parent_pool_name else pool[NAME]
            self.__get_pool_names(pool[POOLS], current_pool_name, pool_names) if pool[POOLS] \
                else pool_names.append(current_pool_name)

    def get_pool_names(self):
        """
        Get the whole pool names. Returns pool name list. Actually called private method
        __get_pool_names(pools, parent_pool_name, pool_names).

        :return: (list) A list object contains whole impala pool name.
        """
        pool_names = []
        self.__get_pool_names(self.__allocations_value[POOLS], ROOT_PARENT_POOL_NAME, pool_names)
        return pool_names

    def __get_pool(self, pools, pool_name):
        """
        Get the configuration of current pool from pools by pool name. If the pool with
        pool_name was found in pools, returns ImpalaPool; otherwise, returns None.

        :param pools: (dict) The whole impala pool configuration.
        :param pool_name: (str) The impala pool name to be found.
        :return: (ImpalaPool or None) A ImpalaPool object if the pool named with pool_name exists.
            otherwise, a None object.
        """
        for pool in pools:
            if DOT_DELIMITER in pool_name:
                parent_pool_name, sub_pool_name = pool_name.split(DOT_DELIMITER, 1)
            else:
                parent_pool_name, sub_pool_name = [pool_name, LEAF_SUB_POOL_NAME]

            if parent_pool_name == pool[NAME]:
                if pool[POOLS]:
                    sub_pool = self.__get_pool(pool[POOLS], sub_pool_name)
                    return sub_pool if sub_pool else None
                else:
                    return ImpalaPool(pool_name, pool)

    def get_pool(self, pool_name):
        """
        Get the configuration of current pool by pool name. Actually called private method
        __get_pool(pools, pool_name).

        :param pool_name: The impala pool name to be found.
        :return: (ImpalaPool or None) A ImpalaPool object if the pool named with pool_name exists.
            otherwise, a None object.
        """
        return self.__get_pool(self.__allocations_value[POOLS], pool_name)

    def get_pools(self):
        """
        Get the configuration of whole pools.

        :return:(dict) A dict object contains the whole ImpalaPool.
        """
        return {pool_name: self.get_pool(pool_name) for pool_name in self.get_pool_names()}

    def update_pools(self, pools_allocated_mem):
        """
        Update the configuration of whole pools.

        :param pools_allocated_mem: (dict) The allocated memory of the pool participating in the scheduling.
        """
        for pool_name, allocated_mem in pools_allocated_mem.items():
            self.get_pool(pool_name).update_pool_mem(allocated_mem)
