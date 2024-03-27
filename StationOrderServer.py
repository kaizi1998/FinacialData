import pandas as pd
from datetime import datetime
from SqliteServer import SqliteServer
from ConfigReader import (database_station_order, station_order_table_huitian, station_order_table_xiaoju,
                          station_order_table_kuaiman, station_order_table_xingxing,
                          station_order_table_processed)
from ConfigReader import station_map


class StationOrderServer:
    def __init__(self):
        self.server = SqliteServer(database_station_order)
        self.cursor = self.server.cursor
        self.df_station_map = station_map()

    # def get_station_order(self, station, platform,
    #                       start_year, start_month, start_day,
    #                       end_year=None, end_month=None, end_day=None):
    #     start_date = datetime(start_year, start_month, start_day).strftime('%Y-%m-%d')
    #     if end_year is None:
    #         end_year = start_year
    #         end_month = start_month
    #         end_day = start_day
    #     end_date = datetime(end_year, end_month, end_day).strftime('%Y-%m-%d')
    #     if platform is station_order_table_huitian:
    #         query = f"""
    #                 SELECT * FROM {platform}
    #                 WHERE strftime('%Y-%m-%d', "开始时间") >= "{start_date}" AND strftime('%Y-%m-%d', "开始时间") <= "{end_date}" AND "站点名称" == "{station}"
    #                 """
    #     elif platform is station_order_table_xiaoju:
    #         query = f"""
    #                 SELECT * FROM {platform}
    #                 WHERE strftime('%Y-%m-%d', "创建时间") >= "{start_date}" AND strftime('%Y-%m-%d', "创建时间") <= "{end_date}" AND "场站名称" == "{station}"
    #                 """
    #     elif platform is station_order_table_kuaiman:
    #         query = f"""
    #                 SELECT * FROM {platform}
    #                 WHERE strftime('%Y-%m-%d', "支付时间") >= "{start_date}" AND strftime('%Y-%m-%d', "支付时间") <= "{end_date}" AND "站点" == "{station}"
    #                 """
    #     elif platform is station_order_table_xingxing:
    #         query = f"""
    #                 SELECT * FROM {platform}
    #                 WHERE strftime('%Y-%m-%d', "充电结束时间") >= "{start_date}" AND strftime('%Y-%m-%d', "充电结束时间") <= "{end_date}" AND "电站名称" == "{station}"
    #                 """
    #     elif platform is station_order_table_processed:
    #         query = f"""
    #                 SELECT * FROM {platform}
    #                 WHERE strftime('%Y-%m-%d', "time") >= "{start_date}" AND strftime('%Y-%m-%d', "time") <= "{end_date}" AND "station" == "{station}"
    #                 """
    #     df = pd.read_sql_query(query, self.server.conn)
    #     df.drop(columns=['platform'], inplace=True)
    #     df = pd.merge(df, self.df_station_map, on=['station_id'], how='inner')
    #     return df

    def update_station_order(self, df: pd.DataFrame, table_name):
        self.server.create_table_from_df(df=df, table_name=table_name)
        self.server.append_new_data_to_table(new_data=df, table_name=table_name)
        if table_name == station_order_table_huitian:
            self.server.deduplicate_sort(table_name, ["开始时间", "流水号"])
        elif table_name == station_order_table_xiaoju:
            self.server.deduplicate_sort(table_name, ["创建时间", "订单ID"])
        elif table_name == station_order_table_kuaiman:
            self.server.deduplicate_sort(table_name, ["支付时间", "账单ID"])
        elif table_name == station_order_table_xingxing:
            self.server.deduplicate_sort(table_name, ["充电结束时间", "平台订单号"])
        elif table_name == station_order_table_processed:
            self.server.deduplicate_sort(table_name, ["time", "station_id", "order_id"])

    def get_table(self, table_name):
        return self.server.get_table(table_name)

    def execute_query(self, sql):
        return self.server.execute_query(sql)

