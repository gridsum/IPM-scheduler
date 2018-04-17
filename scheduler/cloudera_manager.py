from datetime import datetime, timedelta
import logging
import re
import pandas as pd

from scheduler.impala_api_client import ImpalaApiResource
from scheduler.constants import NativeQueryInfoColumn, FormativeQueryInfoColumn
from scheduler.global_utils import convert_mem_unit, spend_time

MEM_LIMIT_REGEX = re.compile(r"MEM_LIMIT=(\d+)")
HOSTS_REGEX = re.compile(r"hosts=(\d+)")

LOGGER = logging.getLogger(__name__)


class ClouderaManager(object):
    """
    The ClouderaManager class that provides methods for get the query information, the configuration of
    impala cluster and update the configuration of impala cluster.
    """

    def __init__(self, server_url, api_version, cluster_name, username, password):
        """
        Creates a ClouderaManager object that provides methods to get and update the query information and
        the configuration of impala cluster.

        :param server_url: (str) The Server url.
        :param api_version: (str) The api version.
        :param cluster_name: (str) The cluster name.
        :param username: (str) The username for login cloudera manager.
        :param password: (str) The password for login cloudera manager.
        """
        self.__api = ImpalaApiResource(server_url, api_version, cluster_name, username, password)

    @classmethod
    def __add_timedelta(cls, gmt):
        """
        Add timedelta on the Greenwich mean time.

        :param gmt: (str) Greenwich mean time.
        :return: (datetime) A datetime object that add 8 hours at greenwich mean time.
        """
        return datetime.strptime(gmt, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(hours=8)

    @classmethod
    def __find_mem_limit(cls, content):
        """
        Search MEM_LIMIT in the given content by regex.

        :param content: (str) The content to be searched.
        :return: (int) A integer value of MEM_LIMIT.
        """
        matcher = MEM_LIMIT_REGEX.search(content)
        return int(matcher.group(1)) if matcher else 0

    @classmethod
    def __find_max_hosts(cls, content):
        """
        Search max hosts in the given content by regex.

        :param content: (str) The content to be searched.
        :return: (int) A integer value of max hosts.
        """
        str_hosts = HOSTS_REGEX.findall(content)
        return max(int(str_host) for str_host in str_hosts) if str_hosts else 0

    def __parse_requires_from_details(self, query_id):
        """
        Parse mem limit and max hosts from query details by query_id.

        :param query_id: (str) The query id.
        :return: (tuple) A tuple object that contains mem limit and max hosts.
        """
        mem_limit, max_hosts = 0, 0
        try:
            query_details_response = self.get_query_details(query_id)

            query_details = query_details_response[NativeQueryInfoColumn.DETAILS]
            mem_limit = ClouderaManager.__find_mem_limit(query_details)
            max_hosts = ClouderaManager.__find_max_hosts(query_details)
        except Exception as e:
            LOGGER.warning(e, "query_id", query_id)
        return convert_mem_unit(mem_limit, "B", "MB"), max_hosts

    def fetch_page_impala_query_info(self, start_time, end_time, filter_str=""):
        """
        Get filtered impala query information by page from the end_time to the start_time.

        :param start_time: (datetime) The start time to fetching query information.
        :param end_time: (datetime) The end time to fetching query information.
        :param filter_str: (str) The filter string to fetch query information.
        :return: (DataFrame) A DataFrame object of fetched query information.
        """
        LOGGER.info("fetching impala query info page data, start_time: %s, end_time: %s" % (start_time, end_time))
        impala_query_response = self.get_impala_queries(start_time, end_time, filter_str)
        queries = impala_query_response[NativeQueryInfoColumn.QUERIES]
        LOGGER.info("impala query info page data size: %d" % len(queries))
        if not queries:
            return None
        df_queries = pd.DataFrame(impala_query_response[NativeQueryInfoColumn.QUERIES])

        sr_query_ids = df_queries[NativeQueryInfoColumn.QUERY_ID]
        sr_start_times = df_queries[NativeQueryInfoColumn.START_TIME].apply(ClouderaManager.__add_timedelta)
        sr_duration_mills = df_queries[NativeQueryInfoColumn.DURATION_MILLIS]
        sr_pools = [x[NativeQueryInfoColumn.POOL] for x in df_queries[NativeQueryInfoColumn.ATTRIBUTES]]
        sr_admission_waits = [x[NativeQueryInfoColumn.ADMISSION_WAIT] for x in df_queries[NativeQueryInfoColumn.ATTRIBUTES]]
        df_base = pd.DataFrame(data={FormativeQueryInfoColumn.QUERY_ID: sr_query_ids,
                                     FormativeQueryInfoColumn.START_TIME: sr_start_times,
                                     FormativeQueryInfoColumn.DURATION_MILLIS: sr_duration_mills,
                                     FormativeQueryInfoColumn.POOL: sr_pools,
                                     FormativeQueryInfoColumn.ADMISSION_WAIT: sr_admission_waits})

        sr_details = sr_query_ids.apply(self.__parse_requires_from_details)
        df_details = pd.DataFrame(data=(x for x in sr_details),
                                  columns=[FormativeQueryInfoColumn.MEM_LIMIT, FormativeQueryInfoColumn.MAX_HOST])
        LOGGER.info("finish fetch impala query info page data, start_time: %s, end_time: %s" % (start_time, end_time))

        return df_base.join(df_details)

    def fetch_impala_query_info(self, start_time, end_time, filter_str):
        """
        Get total filtered impala query information between end_time and start_time.

        :param start_time: (datetime) The start time to fetching query information.
        :param end_time: (datetime) The end time to fetching query information.
        :param filter_str: (str) The filter string to fetch query information.
        :return: (DataFrame) A DataFrame object of total fetched query information.
        """
        LOGGER.info("start fetch impala query info data, start_time: %s, end_time: %s" % (start_time, end_time))
        data = pd.DataFrame()
        while start_time < end_time:
            page_data = self.fetch_page_impala_query_info(start_time, end_time, filter_str)
            if page_data is None:
                break

            data = pd.concat([data, page_data], ignore_index=True)
            end_time = page_data[FormativeQueryInfoColumn.START_TIME].min() - timedelta(milliseconds=1)

        LOGGER.info("finish fetch impala query info data")

        if data.shape[0] == 0:
            return None

        data = data.drop_duplicates([FormativeQueryInfoColumn.QUERY_ID])
        return data

    @spend_time
    def get_impala_queries(self, start_time, end_time, filter_str):
        """
        Get the filtered impala queries between end_time and start_time.

        :param start_time: (datetime) The start time to fetching query information.
        :param end_time: (datetime) The end time to fetching query information.
        :param filter_str: (str) The filter string to fetch query information.
        :return: (dict) A dict object of filtered impala queries between end_time and start_time.
        """
        return self.__api.get_impala_queries(start_time, end_time, filter_str)

    def get_query_details(self, query_id):
        """
        Get the query details by query_id.

        :param query_id: (str) Query id.
        :return: (str) A string object of details text.
        """
        return self.__api.get_query_details(query_id)

    @spend_time
    def get_impala_config(self, view="full"):
        """
        Get the impala configuration.

        :param view: (str) The view to materialize ("full" or "summary").
        :return: (dict) A dict object of impala cluster configuration.
        """
        return self.__api.get_impala_config(view=view)

    @spend_time
    def update_impala_config(self, impala_config):
        """
        Update the impala configuration.

        :param impala_config: (dict) The impala configuration item.
            For example:
                {"items": [{"name": "item_name", "value": "item_value"}]}
        """
        self.__api.update_impala_config(impala_config)

    @spend_time
    def refresh_pools(self):
        """
        Refresh the impala resource pools.
        """
        self.__api.pools_refresh()

    @spend_time
    def get_roles(self):
        """
        Get the whole roles in impala cluster.

        :return: (list) A list of ApiRole objects.
        """
        return self.__api.get_roles()
