import unittest
import os
from scheduler.constants import SCHEDULER_HOME
os.environ[SCHEDULER_HOME] = ""

from tests.utils import get_test_pools_info, get_test_pools_allocated_mem
from scheduler.global_utils import convert_mem_unit, retry, generate_schedule_report_data
from scheduler.base_schedule import PoolStat
from scheduler.priority_schedule import PrioritySchedule


class TestGlobalUtilsMethods(unittest.TestCase):

    def test_converse_unit(self):
        self.assertEqual(convert_mem_unit(1, "GB", "MB"), 1024)
        self.assertEqual(convert_mem_unit(1, "MB", "GB"), 1 / 1024)
        self.assertEqual(convert_mem_unit(1, "TB", "MB"), 1024 * 1024)

    @staticmethod
    def fun1():
        print("test_fun1")

    @staticmethod
    def fun2(str):
        print(str)

    @staticmethod
    def fun3(str):
        print(str)
        raise Exception(str)

    def test_try(self):
        retry(TestGlobalUtilsMethods.fun1)
        retry(TestGlobalUtilsMethods.fun2)(str="test_fun2")
        status = False
        try:
            retry(TestGlobalUtilsMethods.fun3)(str="test_fun3")
        except Exception:
            status = True
        self.assertTrue(status)

    def test_generate_report_dataframe(self):
        pools_stat = {"root.test_pool1": PoolStat("", 10, 10, 10, 0, 500, 0),
                      "root.test_pool2": PoolStat("", 10, 10, 10, 10, 1000, 500),
                      "root.test_pool3": PoolStat("", 10, 10, 10, 0, 500, 0)}
        pools_info = get_test_pools_info(pools_stat)
        schedule = PrioritySchedule()
        pools_allocated_mem = get_test_pools_allocated_mem(schedule, pools_stat)
        report_data = generate_schedule_report_data(pools_info, pools_allocated_mem)
        self.assertTrue(report_data is not None)


if __name__ == "__main__":
    unittest.main()
