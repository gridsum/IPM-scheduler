from scheduler.base_schedule import ScheduleInterface, AbstractSchedule


class DoNothing1Schedule(ScheduleInterface):
    """
    The DoNothing1Schedule class that provides methods for the statistics and the allocated memory of the
    pool participating in the scheduling, which extends from ScheduleInterface.

    By default, this class have a empty implement for both follow methods.
    """
    @classmethod
    def get_pools_stat(cls, df_query_info, stat_start, stat_end):
        return {}

    @classmethod
    def get_pools_allocated_mem(self, schedule_config, pools_info):
        return {}


class DoNothing2Schedule(AbstractSchedule):
    """
    The DoNothing1Schedule class that provides methods for the statistics and the allocated memory of the
    pool participating in the scheduling, which extends from AbstractSchedule.

    By default, this class have a empty implement for the follow method, but a complete implement for method
    get_pools_stat.
    """
    def get_pools_allocated_mem(self, schedule_config, pools_info):
        return {}
