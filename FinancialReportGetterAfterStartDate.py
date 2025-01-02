import functools
import threading

import pandas as pd
from StationOrderServer import StationOrderServer
from ConfigReader import station_order_table_processed, station_map
from datetime import datetime
import concurrent.futures


"""
将最终生成的财务报表按照一个个城市分开汇到一个list上
传输给Generator
"""
class FinancialReportGetterAfterStartDate:
    server = StationOrderServer()
    temp_year = "TempYear"
    temp_month = "TempMonth"
    temp_day = "TempDay"
    temp_station = "TempStation"
    df_station_map = station_map().drop_duplicates(subset=["station_id"], keep='first')
    dic_station_id_start_time = {}
    for i in (df_station_map[['station_id', 'start_time']].drop_duplicates().reset_index(drop=True).to_dict(orient='records')):
        dic_station_id_start_time[i['station_id']] = i['start_time']

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
    def aaa(cls, year=None, month=None, day=None):
        data = {}
        data_lock = threading.Lock()

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_station = {}

            for station_id, start_time in cls.dic_station_id_start_time.items():
                future = executor.submit(cls._process_station, station_id, start_time, year, month, day, data, data_lock)
                future_to_station[future] = station_id

            for future in concurrent.futures.as_completed(future_to_station):
                station_id = future_to_station[future]
                try:
                    future.result()
                except Exception as exc:
                    print(f'Station {cls.dic_station_id_start_time[station_id]} generated an exception: {exc}')

        for city in data.keys():
            cur_df = data[city]
            cur_df = cur_df.drop_duplicates()
            cur_df = cur_df.sort_values(by='start_time')
            cur_df.reset_index(drop=True, inplace=True)
            data[city] = cur_df

    @classmethod
    def _process_station(cls, station_id, start_time, year, month, day, data, data_lock):
        start_time = start_time.replace(".", "-")
        df = cls.get_station_order_reporter(year, month, day, station_id, start_time)

        city = "aaa"
        if not df.empty:
            city = df['city'][0]

        if city == "aaa":
            return

        with data_lock:
            if city not in data.keys():
                data[city] = df
            else:
                data[city] = pd.concat([data[city], df])

    @classmethod
    def get_station_order_reporters(cls, year=None, month=None, day=None):
        data = {}
        for station_id, start_time in cls.dic_station_id_start_time.items():
            if station_id == 169:
                i = 123

            start_time = start_time.replace(".", "-")
            df = cls.get_station_order_reporter(year, month, day, station_id, start_time)

            city = "aaa"
            if not df.empty:
                city = df['city'][0]

            if city == "aaa":
                continue

            if city not in data.keys():
                data[city] = df
            else:
                data[city] = pd.concat([data[city], df])

        for city in data.keys():
            cur_df = data[city]
            cur_df = cur_df.drop_duplicates()
            cur_df = cur_df.sort_values(by=['start_time', 'station_id'])
            cur_df.reset_index(drop=True, inplace=True)
            data[city] = cur_df
        return data

    @classmethod
    def get_station_order_reporter(cls, year, month, day, station_id, start_time):
        df_day = cls._get_station_order_report_day(year, month, day, station_id, start_time)
        df_month = cls._get_station_order_report_month(year, month, day, station_id, start_time)
        df_year = cls._get_station_order_report_year(year, month, day, station_id, start_time)
        df_year.drop(columns=["time"], inplace=True)
        df_month.drop(columns=["time"], inplace=True)
        df = pd.merge(df_year, df_month, on=["station_id"], how='left')
        df = pd.merge(df, df_day, on=["station_id"], how='left')

        df['station_id'] = df['station_id'].astype(int)
        df.sort_values(by=["station_id"], inplace=True)
        df = pd.merge(df, cls.df_station_map, on=["station_id"], how='inner')

        df["total_power"] = df["total_power"].astype(int)

        df["day_utilize_hours"] = df["day_charging_capacity(kwh)"] / df["total_power"]
        df["month_utilize_hours"] = df["month_charging_capacity(kwh)"] / df["total_power"]
        df["year_utilize_hours"] = df["year_charging_capacity(kwh)"] / df["total_power"]

        df["day_average_service_fee_per_kwh(RMB)"] = df["day_service_fee(RMB)"] / df["day_charging_capacity(kwh)"]

        df = df.fillna(0)
        return df

    @classmethod
    @_drop_temp_table
    def _get_station_order_report_day(cls, year, month, day, station_id, start_time):
        date_obj = datetime(year, month, day)
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
        WHERE station_id == "{station_id}" 
            AND strftime('%Y-%m-%d', time) == "{cur_date}"
            AND strftime('%Y-%m-%d', time) >= "{start_time}"
        GROUP BY strftime('%Y-%m-%d', time)
        ORDER BY "time";
        """
        cls.server.execute_query(sql)
        return cls.server.get_table(cls.temp_day)

    @classmethod
    @_drop_temp_table
    def _get_station_order_report_month(cls, year, month, day, station_id, start_time):
        month_obj = datetime(year, month, day)
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
        WHERE station_id == "{station_id}" 
            AND strftime('%Y-%m', time) == "{cur_month}" 
            AND strftime('%Y-%m-%d', time) <= "{cur_date}"
            AND strftime('%Y-%m-%d', time) >= "{start_time}"
        GROUP BY strftime('%Y-%m', time)
        ORDER BY "time";
        """
        cls.server.execute_query(sql)
        return cls.server.get_table(cls.temp_day)

    @classmethod
    @_drop_temp_table
    def _get_station_order_report_year(cls, year, month, day, station_id, start_time):
        year_obj = datetime(year, month, day)
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
        WHERE station_id == "{station_id}" 
            AND strftime('%Y', time) == "{cur_year}" 
            AND strftime('%Y-%m-%d', time) <= "{cur_date}"
            AND strftime('%Y-%m-%d', time) >= "{start_time}"
        GROUP BY strftime('%Y', time)
        ORDER BY "time";
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

