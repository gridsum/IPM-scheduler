import unittest

import os
from scheduler.constants import SCHEDULER_HOME
os.environ[SCHEDULER_HOME] = ""

from scheduler.priority_schedule import PrioritySchedule
from scheduler.base_schedule import PoolStat
from tests.utils import get_test_pools_allocated_mem


class TestPriorityScheduleMethods(unittest.TestCase):

    def test_schedule_case1(self):
        """
        test the pool(root.test_pool2) move 100MB to the pool(root.test_pool1)
        :return:
        """
        pools_stat = {"root.test_pool1": PoolStat("", 10, 10, 10, 10, 100, 100)}
        pools_allocated_mem = get_test_pools_allocated_mem(PrioritySchedule, pools_stat)

        self.assertEqual(pools_allocated_mem["root.test_pool1"], 1100)
        self.assertEqual(pools_allocated_mem["root.test_pool2"], 900)

    def test_schedule_case2(self):
        """
        test the pool(root.test_pool2) move 300MB to the pool(root.test_pool1)
        test the pool(root.test_pool2) move 500MB to the pool(root.test_pool3)
        :return:
        """
        pools_stat = {"root.test_pool1": PoolStat("", 10, 10, 10, 10, 100, 500),
                      "root.test_pool3": PoolStat("", 10, 10, 10, 10, 100, 500)}
        pools_allocated_mem = get_test_pools_allocated_mem(PrioritySchedule, pools_stat)

        self.assertEqual(pools_allocated_mem["root.test_pool1"], 1300)
        self.assertEqual(pools_allocated_mem["root.test_pool2"], 200)
        self.assertEqual(pools_allocated_mem["root.test_pool3"], 1500)

    def test_schedule_case3(self):
        """
        test the pool(root.test_pool1) move 400MB to the pool(root.test_pool2)
        test the pool(root.test_pool3) move 500MB to the pool(root.test_pool2)
        :return:
        """
        pools_stat = {"root.test_pool1": PoolStat("", 10, 10, 10, 0, 500, 0),
                      "root.test_pool2": PoolStat("", 10, 10, 10, 10, 1000, 500),
                      "root.test_pool3": PoolStat("", 10, 10, 10, 0, 500, 0)}
        pools_allocated_mem = get_test_pools_allocated_mem(PrioritySchedule, pools_stat)

        self.assertEqual(pools_allocated_mem["root.test_pool1"], 600)
        self.assertEqual(pools_allocated_mem["root.test_pool2"], 1500)
        self.assertEqual(pools_allocated_mem["root.test_pool3"], 900)

    def test_schedule_case4(self):
        """
        test all pools busy case
        :return:
        """
        pools_stat = {"root.test_pool1": PoolStat("", 10, 10, 10, 10, 900, 110),
                      "root.test_pool2": PoolStat("", 10, 10, 10, 10, 1000, 500),
                      "root.test_pool3": PoolStat("", 10, 10, 10, 10, 1000, 0)}
        pools_allocated_mem = get_test_pools_allocated_mem(PrioritySchedule, pools_stat)

        self.assertEqual(pools_allocated_mem, {})

    def test_schedule_case5(self):
        """
        test all pools idle case
        :return:
        """
        pools_stat = {"root.test_pool1": PoolStat("", 10, 10, 10, 0, 500, 0),
                      "root.test_pool2": PoolStat("", 10, 10, 10, 0, 500, 0),
                      "root.test_pool3": PoolStat("", 10, 10, 0, 0, 0, 0)}
        pools_allocated_mem = get_test_pools_allocated_mem(PrioritySchedule, pools_stat)

        self.assertEqual(pools_allocated_mem, {})

if __name__ == "__main__":
    unittest.main()
