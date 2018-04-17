import unittest
import pandas as pd
from datetime import datetime, timedelta
from scheduler.base_schedule import PoolStat, AbstractSchedule


class TestAbstractScheduleMethods(unittest.TestCase):

    def setUp(self):
        self.abstract_schedule = AbstractSchedule()

    def test_get_pools_stat(self):
        stat_start = datetime.strptime("2018-02-24 11:00:00", "%Y-%m-%d %H:%M:%S")
        df = pd.read_csv("./resources/query_info_data_test.csv")
        df["start_time"] = df["start_time"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"))
        stat_end = stat_start + timedelta(minutes=10)
        pools_stat = self.abstract_schedule.get_pools_stat(df, stat_start, stat_end)
        pool_stat = pools_stat.get("test_pool1", PoolStat("test_pool1", 0, 0, 0, 0, 0, 0))
        self.assertEqual(pool_stat.query_total, 2)
        self.assertEqual(pool_stat.wait_query_total, 1)
        self.assertEqual(pool_stat.run_secs, 15.0)
        self.assertEqual(pool_stat.wait_secs, 5.0)
        self.assertEqual(pool_stat.used_mem_avg, 23333)
        self.assertEqual(pool_stat.wait_mem_avg, 17500)


if __name__ == "__main__":
    unittest.main()
