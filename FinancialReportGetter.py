import functools
import pandas as pd
from StationOrderServer import StationOrderServer
from ConfigReader import station_order_table_processed, station_map
from datetime import datetime


"""
将最终生成的财务报表按照一个个城市分开汇到一个list上
传输给Generator
"""
class FinancialReportGetter:
    server = StationOrderServer()
    temp_year = "TempYear"
    temp_month = "TempMonth"
    temp_day = "TempDay"
    temp_station = "TempStation"
    df_station_map = station_map().drop_duplicates(subset=["station_id"], keep='first')

    @staticmethod
    def _drop_temp_table(func):
        @functools.wraps(func)
        def wrapper(cls, *args, **kwargs):
            temp_tables = [cls.temp_year, cls.temp_month, cls.temp_day, cls.temp_station]
            for table_name in temp_tables:
                # 检查临时表是否存在
                cls.server.execute_query(f"SELECT name FROM sqlite_temp_master WHERE type='table' AND name='{table_name}';")
                result = cls.server.cursor.fetchone()

                # 如果存在，删除该临时表
                if result:
                    cls.server.execute_query(f"DROP TABLE {table_name};")
            return func(cls, *args, **kwargs)
        return wrapper

    @classmethod
    def get_station_order_reporters(cls, year=None, month=None, day=None):
        df = cls.get_station_order_reporter(year, month, day)
        df['station_id'] = df['station_id'].astype(int)
        df.sort_values(by=["station_id"], inplace=True)
        df = pd.merge(df, cls.df_station_map, on=["station_id"], how='inner')

        # test
        df["total_power"] = df["total_power"].astype(int)

        df["day_utilize_hours"] = df["day_charging_capacity(kwh)"]/df["total_power"]
        df["month_utilize_hours"] = df["month_charging_capacity(kwh)"]/df["total_power"]
        df["year_utilize_hours"] = df["year_charging_capacity(kwh)"]/df["total_power"]

        df["day_average_service_fee_per_kwh(RMB)"] = df["day_service_fee(RMB)"] / df["day_charging_capacity(kwh)"]

        df = df.fillna(0)
        data = {}
        cities = df['city'].unique()
        for city in cities:
            cur_df = df[df['city'] == city]
            cur_df = cur_df.sort_values(by='start_time')
            cur_df.reset_index(drop=True, inplace=True)
            data[city] = cur_df
        return data

    @classmethod
    def get_station_order_reporter(cls, year, month, day):
        df_station = cls._get_station(year)
        df_day = cls._get_station_order_report_day(year, month, day)
        df_month = cls._get_station_order_report_month(year, month, day)
        df_year = cls._get_station_order_report_year(year, month, day)
        # df_year["year"] = pd.to_datetime(df_year["time"]).dt.year
        df_year.drop(columns=["time"], inplace=True)
        # df_month["year"] = pd.to_datetime(df_month['time'], format='%Y-%m').dt.year
        df_month.drop(columns=["time"], inplace=True)
        df_final = pd.merge(df_station, df_day, on=["station_id"], how='left')
        df_final = pd.merge(df_final, df_month, on=["station_id"], how='left')
        df_final = pd.merge(df_final, df_year, on=["station_id"], how='left')
        return df_final

    @classmethod
    @_drop_temp_table
    def _get_station_order_report_day(cls, year, month, day):
        # 创建datetime对象
        date_obj = datetime(year, month, day)
        # 转换为"YYYY-mm-dd"格式的字符串
        cur_date = date_obj.strftime('%Y-%m-%d')
        source_table = station_order_table_processed
        sql = f"""
        CREATE TEMP TABLE {cls.temp_day} AS
        SELECT
            strftime('%Y-%m-%d', time) AS "time", 
            station_id AS "station_id",
            COUNT(*) AS "day_charged_count",
            SUM("charging_capacity(kwh)") AS "day_charging_capacity(kwh)", 
            SUM("electric_fee(RMB)") AS "day_electric_fee(RMB)",
            SUM("service_fee(RMB)") AS "day_service_fee(RMB)"
        FROM {source_table}
        WHERE strftime('%Y-%m-%d', time) == "{cur_date}"
        GROUP BY strftime('%Y-%m-%d', time), station_id
        ORDER BY "time", "station_id";
        """
        cls.server.execute_query(sql)
        return cls.server.get_table(cls.temp_day)

    @classmethod
    @_drop_temp_table
    def _get_station_order_report_month(cls, year, month, day):
        # 创建datetime对象
        month_obj = datetime(year, month, day)
        # 转换为"YYYY-mm-dd"格式的字符串
        cur_date = month_obj.strftime('%Y-%m-%d')
        cur_month = month_obj.strftime('%Y-%m')
        source_table = station_order_table_processed
        sql = f"""
        CREATE TEMP TABLE {cls.temp_day} AS
        SELECT
            strftime('%Y-%m', time) AS "time", 
            station_id AS "station_id",
            COUNT(*) AS "month_charged_count",
            SUM("charging_capacity(kwh)") AS "month_charging_capacity(kwh)", 
            SUM("electric_fee(RMB)") AS "month_electric_fee(RMB)",
            SUM("service_fee(RMB)") AS "month_service_fee(RMB)"
        FROM {source_table}
        WHERE strftime('%Y-%m', time) == "{cur_month}" AND strftime('%Y-%m-%d', time) <= "{cur_date}"
        GROUP BY strftime('%Y-%m', time), station_id
        ORDER BY "time", "station_id";
        """
        cls.server.execute_query(sql)
        return cls.server.get_table(cls.temp_day)

    # SUM("service_fee(RMB)")/NULLIF(SUM("charging_capacity(kwh)"), 0) AS "year_average_service_fee_per_kwh(RMB)"
    @classmethod
    @_drop_temp_table
    def _get_station_order_report_year(cls, year, month, day):
        # 创建datetime对象
        year_obj = datetime(year, month, day)
        # 转换为"YYYY-mm-dd"格式的字符串
        cur_year = year_obj.strftime('%Y')
        cur_date = year_obj.strftime('%Y-%m-%d')
        source_table = station_order_table_processed
        sql = f"""
        CREATE TEMP TABLE {cls.temp_day} AS
        SELECT
            strftime('%Y', time) AS "time", 
            station_id AS "station_id",
            COUNT(*) AS "year_charged_count",
            SUM("charging_capacity(kwh)") AS "year_charging_capacity(kwh)", 
            SUM("electric_fee(RMB)") AS "year_electric_fee(RMB)",
            SUM("service_fee(RMB)") AS "year_service_fee(RMB)"
        FROM {source_table}
        WHERE strftime('%Y', time) == "{cur_year}" AND strftime('%Y-%m-%d', time) <= "{cur_date}"
        GROUP BY strftime('%Y', time), station_id
        ORDER BY "time", "station_id";
        """
        cls.server.execute_query(sql)
        return cls.server.get_table(cls.temp_day)

    @classmethod
    @_drop_temp_table
    def _get_station(cls, year):
        source_table = station_order_table_processed
        sql = f"""
        CREATE TEMP TABLE {cls.temp_station} AS
        SELECT
            station_id AS "station_id"
        FROM {source_table}
        WHERE time < "{year+1}-01-01 00:00:00" 
        GROUP BY station_id
        ORDER BY "station_id";
        """
        cls.server.execute_query(sql)
        return cls.server.get_table(cls.temp_station)

    @classmethod
    @_drop_temp_table
    def _get_station_order_report_day_city(cls, year, month, day):
        # 创建datetime对象
        date_obj = datetime(year, month, day)
        # 转换为"YYYY-mm-dd"格式的字符串
        cur_date = date_obj.strftime('%Y-%m-%d')
        source_table = station_order_table_processed
        sql = f"""
        CREATE TEMP TABLE {cls.temp_day} AS
        SELECT
            strftime('%Y-%m-%d', time) AS "time", 
            city as "city",
            COUNT(*) AS "day_charged_count",
            SUM("charging_capacity(kwh)") AS "day_charging_capacity(kwh)", 
            SUM("electric_fee(RMB)") AS "day_electric_fee(RMB)",
            SUM("service_fee(RMB)") AS "day_service_fee(RMB)"
        FROM {source_table}
        WHERE strftime('%Y-%m-%d', time) == "{cur_date}"
        GROUP BY strftime('%Y-%m-%d', time), city
        ORDER BY "time", "city";
        """
        cls.server.execute_query(sql)
        return cls.server.get_table(cls.temp_day)

    @classmethod
    def get_station_order_report_day_city(cls, year, month, day):
        df1 = cls._get_city(year)
        df2 = cls._get_station_order_report_day_city(year, month, day)
        df_final = pd.merge(df1, df2, on=["city"], how='left')
        return df_final

    @classmethod
    @_drop_temp_table
    def _get_city(cls, year):
        source_table = station_order_table_processed
        sql = f"""
        CREATE TEMP TABLE {cls.temp_station} AS
        SELECT
            city AS "city"
        FROM {source_table}
        WHERE time < "{year+1}-01-01 00:00:00"
        GROUP BY city
        ORDER BY "city";
        """
        cls.server.execute_query(sql)
        return cls.server.get_table(cls.temp_station)
    #
    # @classmethod
    # @_drop_temp_table
    # def get_full_order_by_date_station(cls, year, month, day, station):
    #     # 创建datetime对象
    #     date_obj = datetime(year, month, day)
    #     # 转换为"YYYY-mm-dd"格式的字符串
    #     cur_date = date_obj.strftime('%Y-%m-%d')
    #     source_table = station_order_table_processed
    #     sql = f"""
    #     CREATE TEMP TABLE {cls.temp_day} AS
    #     SELECT *
    #     FROM {source_table}
    #     WHERE strftime('%Y-%m-%d', time) == "{cur_date}" AND station == "{station}"
    #     ORDER BY "time";
    #     """
    #     cls.server.execute_query(sql)
    #     return cls.server.get_table(cls.temp_day)
