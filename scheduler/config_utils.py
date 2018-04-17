import yaml
import os

from scheduler.constants import SCHEDULER_HOME


class ConfigUtils(object):
    """
    ConfigUtils object that provides methods for read and write yaml configuration files.
    """

    @staticmethod
    def read(path):
        """
        Read the contents of yaml configuration and fill the environment variables.

        :param path: (str) The path of yaml configuration file.
        :return: (dict) A dict object of yaml configuration.
        """
        with open(path, "r") as f:
            content = f.read()
            config = content.replace("${%s}" % SCHEDULER_HOME, os.environ[SCHEDULER_HOME])

        return yaml.load(config)

    @staticmethod
    def write(path, config):
        """
        Write the contents to yaml configuration file.

        :param path: (str) The path of yaml configuration file.
        :param config: (dict) The configuration contents.
        """
        with open(path, "w") as f:
            yaml.dump(config, f)
