import requests


class ImpalaApiResource(object):
    """
    The ImpalaApiResource class that provides methods for get and update the resources from cloudera manager.
    """

    def __init__(self, server_url, api_version, cluster_name, username, password):
        """
        Creates a ImpalaApiResource object that provides methods to get and update resources.

        :param server_url: (str) The Server url.
        :param api_version: (str) The api version.
        :param cluster_name: (str) The cluster name.
        :param username: (str) The username for login cloudera manager.
        :param password: (str) The password for login cloudera manager.
        """
        self.__base_path = "%s/api/%s/clusters/%s" % (server_url, api_version, cluster_name)
        self.__session = requests.Session()
        self.__session.get(self.__base_path, auth=(username, password))

    def __del__(self):
        """
        Delete the session.
        """
        self.__session.close()

    @classmethod
    def __check_status_code(cls, status_code):
        """
        Check the status code of http response is OK. if status code is equals or greater than 400,
        an IOError will be raise.

        :param status_code: The status code of response.
        """
        if status_code >= 400:
            raise IOError("error status_code: %d" % status_code)

    def get_impala_queries(self, start_time, end_time, filter_str=""):
        """
        Get the filtered impala queries between end_time and start_time.

        By default, filter_str is "", page limit is 100 and offset is 0.

        :param start_time: (datetime) The start time to fetching query information.
        :param end_time: (datetime) The end time to fetching query information.
        :param filter_str: (str) The filter string to fetch query information.
        :return: (json) A json object of the response.
        """
        path = "%s/services/impala/impalaQueries" % self.__base_path
        params = {"filter": filter_str, "to": end_time.isoformat(),
                  "from": start_time.isoformat(), "limit": 100, "offset": 0}
        response = self.__session.get(path, params=params)
        ImpalaApiResource.__check_status_code(response.status_code)
        return response.json()

    def get_query_details(self, query_id):
        """
        Get the query details by query_id.

        :param query_id: (str) The query id.
        :return: (json) A json object of the response.
        """
        path = "%s/services/impala/impalaQueries/%s" % (self.__base_path, query_id)
        response = self.__session.get(path)
        ImpalaApiResource.__check_status_code(response.status_code)
        return response.json()

    def get_impala_config(self, view=None):
        """
        Get the impala configuration.

        :param view: (str) The view to materialize ("full" or "summary")
        :return: (json) A json object of the response.
        """
        path = "%s/services/impala/config" % self.__base_path
        response = self.__session.get(path, params=view and dict(view=view) or None)
        ImpalaApiResource.__check_status_code(response.status_code)
        return response.json()

    def update_impala_config(self, impala_config):
        """
        Update the impala configuration.

        :param impala_config: (dict) The updated impala configuration.
        :return: (json) A json object of the response.
        """
        path = "%s/services/impala/config" % self.__base_path
        response = self.__session.put(path, data=impala_config, headers={"Content-Type": "application/json"})
        ImpalaApiResource.__check_status_code(response.status_code)
        return response.json()

    def pools_refresh(self):
        """
        Refresh the impala resource pools.

        :return: (json) A json object of the response.
        """
        path = "%s/commands/poolsRefresh" % self.__base_path
        response = self.__session.post(path)
        self.__check_status_code(response.status_code)
        return response.json()

    def get_roles(self):
        """
        Get the whole roles in impala cluster.

        :return: (json) A json object of the response.
        """
        path = "%s/services/impala/roles" % self.__base_path
        response = self.__session.get(path)
        self.__check_status_code(response.status_code)
        return response.json()
