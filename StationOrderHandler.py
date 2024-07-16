import pandas as pd
import re
from ConfigReader import station_map


class StationOrderHandler:
    df_station_map = station_map()
    df_station_map = pd.DataFrame(df_station_map[['station_id', 'station_name_platform', 'platform', 'city']])

    @classmethod
    def huitian(cls, df: pd.DataFrame):
        platform = "汇天"
        l = ["所有汇天公司车队", "平台测试账户–曾一纯", "平台测试账户–蒋旭", "平台测试账户–周楚然"]
        df.drop("序号", axis=1, inplace=True)
        df["流水号"] = df["流水号"].astype(str)
        df = df[(~df["会员名称"].isin(l)) & (df["流水号"] != "0")]
        # columns = list(df.columns)
        # if "桩体号" in columns:
        #     df["桩体号"] = df["桩体号"].astype(str)
        # if "手机号" in columns:
        #     df["手机号"] = df["手机号"].astype(str)
        # if "卡号" in columns:
        #     df["卡号"] = df["卡号"].astype(str)
        # if "桩流水号" in columns:
        #     df["桩流水号"] = df["桩流水号"].astype(str)
        # if "用户流水号" in columns:
        #     df["用户流水号"] = df["用户流水号"].astype(str)
        # if "会员ID" in columns:
        #     df["会员ID"] = df["会员ID"].astype(str)
        df["开始时间"] = pd.to_datetime(df["开始时间"])
        df = df.drop_duplicates(subset=['流水号', '开始时间'])
        # 选择你想要的列并给它们指定新的列名
        # 假设你想选择名为 'old_column_name1' 和 'old_column_name2' 的列，并分别重命名为 'new_column_name1' 和 'new_column_name2'
        selected_columns = {
            '开始时间': 'time',
            '站点名称': 'station_name_platform',
            '流水号': 'order_id',
            '电量': 'charging_capacity(kwh)',
            '实收电费(元)': 'electric_fee(RMB)',
            '实收服务费（元）': 'service_fee(RMB)'
        }
        df = df[selected_columns.keys()].rename(columns=selected_columns)
        df.insert(2, 'platform', platform)
        df = pd.merge(df, cls.df_station_map, on=['station_name_platform', "platform"], how='inner')
        df.drop(columns=['station_name_platform', "platform"], inplace=True)
        df.sort_values(by=['time', 'station_id', 'order_id'], inplace=True)
        return df

    @classmethod
    def xiaoju(cls, df: pd.DataFrame):
        platform = "小桔"
        df = df.drop_duplicates(subset=['订单ID', '创建时间'])
        # 选择你想要的列并给它们指定新的列名
        # 假设你想选择名为 'old_column_name1' 和 'old_column_name2' 的列，并分别重命名为 'new_column_name1' 和 'new_column_name2'
        selected_columns = {
            '创建时间': 'time',
            '场站名称': 'station_name_platform',
            '订单ID': 'order_id',
            '充电量（度）': 'charging_capacity(kwh)',
            '充电电费（元）': 'electric_fee(RMB)',
            '充电服务费（元）': 'service_fee(RMB)'
        }
        df = df[selected_columns.keys()].rename(columns=selected_columns)
        df.insert(2, 'platform', platform)
        df = pd.merge(df, cls.df_station_map, on=['station_name_platform', "platform"], how='inner')
        df.drop(columns=['station_name_platform', "platform"], inplace=True)
        df.sort_values(by=['time', 'station_id', 'order_id'], inplace=True)
        return df

    @classmethod
    def xingxing_old(cls, df: pd.DataFrame):
        platform = "星星"
        df = df.drop_duplicates(subset=['平台订单号', '实际充电结束时间'])
        # 选择你想要的列并给它们指定新的列名
        # 假设你想选择名为 'old_column_name1' 和 'old_column_name2' 的列，并分别重命名为 'new_column_name1' 和 'new_column_name2'
        selected_columns = {
            '充电结束时间': 'time',
            '电站名称': 'station_name_platform',
            '平台订单号': 'order_id',
            '订单电量(度)': 'charging_capacity(kwh)',
            '订单电费(元)': 'electric_fee(RMB)',
            '订单服务费(元)': 'service_fee(RMB)'
        }
        df = df[selected_columns.keys()].rename(columns=selected_columns)
        df.insert(2, 'platform', platform)
        df = pd.merge(df, cls.df_station_map, on=['station_name_platform', "platform"], how='inner')
        df.drop(columns=['station_name_platform', "platform"], inplace=True)
        df.sort_values(by=['time', 'station_id', 'order_id'], inplace=True)
        return df

    @classmethod
    def xingxing_new(cls, df: pd.DataFrame):
        platform = "星星"
        df = df.drop_duplicates(subset=['平台订单号', '充电结束时间'])
        # 选择你想要的列并给它们指定新的列名
        # 假设你想选择名为 'old_column_name1' 和 'old_column_name2' 的列，并分别重命名为 'new_column_name1' 和 'new_column_name2'
        selected_columns = {
            '充电结束时间': 'time',
            '电站名称': 'station_name_platform',
            '平台订单号': 'order_id',
            '订单电量': 'charging_capacity(kwh)',
            '电费': 'electric_fee(RMB)',
            '服务费': 'service_fee(RMB)'
        }
        df = df[selected_columns.keys()].rename(columns=selected_columns)
        df.insert(2, 'platform', platform)
        df = pd.merge(df, cls.df_station_map, on=['station_name_platform', "platform"], how='inner')
        df.drop(columns=['station_name_platform', "platform"], inplace=True)
        df.sort_values(by=['time', 'station_id', 'order_id'], inplace=True)
        return df

    @classmethod
    def kuaiman_new(cls, df: pd.DataFrame):
        platform = "快满"
        df["订单ID"] = df["订单ID"].astype(str)
        df["支付时间"] = pd.to_datetime(df["支付时间"])
        df = df.drop_duplicates(subset=['订单ID', '支付时间'])
        # 选择你想要的列并给它们指定新的列名
        # 假设你想选择名为 'old_column_name1' 和 'old_column_name2' 的列，并分别重命名为 'new_column_name1' 和 'new_column_name2'
        selected_columns = {
            '支付时间': 'time',
            '站点': 'station_name_platform',
            '订单ID': 'order_id',
            '总电量': 'charging_capacity(kwh)',
            '实付电费': 'electric_fee(RMB)',
            '实付服务费': 'service_fee(RMB)'
        }
        df = df[selected_columns.keys()].rename(columns=selected_columns)
        df.insert(2, 'platform', platform)
        df = pd.merge(df, cls.df_station_map, on=['station_name_platform', "platform"], how='inner')
        df.drop(columns=['station_name_platform', "platform"], inplace=True)
        df.sort_values(by=['time', 'station_id', 'order_id'], inplace=True)
        return df

    @classmethod
    def kuaiman_old(cls, df: pd.DataFrame):
        platform = "快满"
        df["账单ID"] = df["账单ID"].astype(str)
        df["支付时间"] = pd.to_datetime(df["支付时间"])
        df = df.drop_duplicates(subset=['账单ID', '支付时间'])
        # 选择你想要的列并给它们指定新的列名
        # 假设你想选择名为 'old_column_name1' 和 'old_column_name2' 的列，并分别重命名为 'new_column_name1' 和 'new_column_name2'
        selected_columns = {
            '支付时间': 'time',
            '站点': 'station_name_platform',
            '账单ID': 'order_id',
            '总电量': 'charging_capacity(kwh)',
            '总电费': 'electric_fee(RMB)',
            '总服务费': 'service_fee(RMB)'
        }
        df = df[selected_columns.keys()].rename(columns=selected_columns)
        df.insert(2, 'platform', platform)
        df = pd.merge(df, cls.df_station_map, on=['station_name_platform', "platform"], how='inner')
        df.drop(columns=['station_name_platform', "platform"], inplace=True)
        df.sort_values(by=['time', 'station_id', 'order_id'], inplace=True)
        return df

    @staticmethod
    def _convert_to_hours(time_str):
        # 使用正则表达式匹配小时、分钟和秒
        hours_match = re.search(r'(\d+)小时', time_str)
        minutes_match = re.search(r'(\d+)分', time_str)
        seconds_match = re.search(r'(\d+)秒', time_str)

        hours = int(hours_match.group(1)) if hours_match else 0
        minutes = int(minutes_match.group(1)) if minutes_match else 0
        seconds = int(seconds_match.group(1)) if seconds_match else 0

        # 将时间转换为小时
        total_hours = hours + minutes / 60 + seconds / 3600
        return total_hours
