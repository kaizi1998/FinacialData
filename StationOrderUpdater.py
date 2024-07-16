import os
import pandas as pd
import chardet
from ConfigReader import entrance_huitian, entrance_xiaoju, entrance_xingxing_old, entrance_xingxing_new, entrance_kuaiman_new, entrance_kuaiman_old
from ConfigReader import table_file_have_read, station_order_table_huitian, station_order_table_xiaoju, station_order_table_kuaiman, station_order_table_xingxing, station_order_table_processed
from StationOrderServer import StationOrderServer
from StationOrderHandler import StationOrderHandler


class StationDataUpdater:
    def __init__(self):
        self.data_server = StationOrderServer()
        self._sqlite_server = self.data_server.server

    def get_file_have_read(self, platform):
        is_table_exist = self._sqlite_server.is_table_exist(table_file_have_read)
        file_have_read = []
        if is_table_exist:
            query = f"""
                    SELECT * FROM {table_file_have_read}
                    WHERE platform == "{platform}"
                    """
            df = pd.read_sql_query(query, self._sqlite_server.conn)
            file_have_read = set(df["file_name"])
        return file_have_read

    def update_file_have_read(self, platform, file_list):
        if len(file_list) > 0:
            platform_list = [platform] * len(file_list)
            df = pd.DataFrame({'platform': platform_list, 'file_name': file_list})
            self._sqlite_server.create_table_from_df(df, table_file_have_read)
            self._sqlite_server.append_new_data_to_table(new_data=df, table_name=table_file_have_read)
            self._sqlite_server.deduplicate_sort(table_file_have_read, ["platform", "file_name"])

    @classmethod
    def _detect_encoding(cls, file_path):
        with open(file_path, 'rb') as rawdata:
            result = chardet.detect(rawdata.read(10000))  # 读取文件的前一部分来检测编码
        return result['encoding']

    def huitian(self):
        platform = station_order_table_huitian
        folder_path = entrance_huitian
        file_have_read = self.get_file_have_read(platform)
        file_new_read = set()
        # 遍历文件夹
        for filename in os.listdir(folder_path):
            if (filename.endswith('.xlsx') and not (filename.startswith('~') or filename.startswith('$'))) and filename not in file_have_read:
                print(f"读取文件{filename}")
                file_path = os.path.join(folder_path, filename)
                df = pd.read_excel(file_path, skiprows=1, skipfooter=2, engine="openpyxl")
                # cls.data_server.update_station_order(df=df, table_name=station_order_table_huitian)
                self.data_server.update_station_order(df=StationOrderHandler.huitian(df),
                                                      table_name=station_order_table_processed)
                file_new_read.add(filename)
        file_new_read = list(file_new_read)
        self.update_file_have_read(platform, file_new_read)


    def xiaoju(self):
        platform = station_order_table_xiaoju
        folder_path = entrance_xiaoju
        file_have_read = self.get_file_have_read(platform)
        file_new_read = set()
        # 遍历文件夹
        for filename in os.listdir(folder_path):
            if (filename.endswith('.xlsx') and not (filename.startswith('~') or filename.startswith('$'))) and filename not in file_have_read:
                print(f"读取文件{filename}")
                file_path = os.path.join(folder_path, filename)
                df = pd.read_excel(file_path, engine="openpyxl")
                df = df[df["订单来源"] != "中核汇天"]
                df["创建时间"] = pd.to_datetime(df["创建时间"])
                df["订单ID"] = df["订单ID"].astype(str)
                df["外部订单ID"] = df["外部订单ID"].astype(str)
                df["充电桩ID"] = df["充电桩ID"].astype(str)
                df["充电枪ID"] = df["充电枪ID"].astype(str)
                # cls.data_server.update_station_order(df=df, table_name=station_order_table_xiaoju)
                self.data_server.update_station_order(df=StationOrderHandler.xiaoju(df),
                                                     table_name=station_order_table_processed)
                file_new_read.add(filename)
        file_new_read = list(file_new_read)
        self.update_file_have_read(platform, file_new_read)

    def xingxing_old(self):
        platform = station_order_table_xingxing
        folder_path = entrance_xingxing_old
        file_have_read = self.get_file_have_read(platform)
        file_new_read = set()
        # 遍历文件夹
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv') and filename not in file_have_read:
                print(f"读取文件{filename}")
                file_path = os.path.join(folder_path, filename)
                file_encoding = self._detect_encoding(file_path)
                df = pd.read_csv(file_path, skiprows=3, encoding=file_encoding)
                df["电站名称"] = df["电站名称"].str.strip()
                df["实际充电结束时间"] = pd.to_datetime(df["实际充电结束时间"].str.strip(), format="%Y.%m.%d %H:%M:%S")
                df["平台订单号"] = df["平台订单号"].astype(str).str.strip()
                df["业务订单号"] = df["业务订单号"].astype(str).str.strip()
                df["枪编号"] = df["枪编号"].astype(str).str.strip()
                # cls.data_server.update_station_order(df=df, table_name=station_order_table_xingxing)
                self.data_server.update_station_order(df=StationOrderHandler.xingxing_old(df),
                                                     table_name=station_order_table_processed)
                file_new_read.add(filename)
        file_new_read = list(file_new_read)
        self.update_file_have_read(platform, file_new_read)

    def xingxing_new(self):
        platform = station_order_table_xingxing
        folder_path = entrance_xingxing_new
        file_have_read = self.get_file_have_read(platform)
        file_new_read = set()
        # 遍历文件夹
        for filename in os.listdir(folder_path):
            if (filename.endswith('.xlsx') and not (filename.startswith('~') or filename.startswith('$'))) and filename not in file_have_read:
                print(f"读取文件{filename}")
                file_path = os.path.join(folder_path, filename)
                df = pd.read_excel(file_path, engine="openpyxl")
                df["电站名称"] = df["电站名称"].str.strip()
                df["充电结束时间"] = pd.to_datetime(df["充电结束时间"].str.strip(), format="%Y-%m-%d %H:%M:%S")
                df["平台订单号"] = df["平台订单号"].astype(str).str.strip()
                df["业务订单号"] = df["业务订单号"].astype(str).str.strip()
                df["枪编号"] = df["枪编号"].astype(str).str.strip()
                self.data_server.update_station_order(df=StationOrderHandler.xingxing_new(df),
                                                     table_name=station_order_table_processed)
                file_new_read.add(filename)
        file_new_read = list(file_new_read)
        self.update_file_have_read(platform, file_new_read)

    def kuaiman_old(self):
        platform = station_order_table_kuaiman
        folder_path = entrance_kuaiman_old
        file_have_read = self.get_file_have_read(platform)
        file_new_read = set()
        # 遍历文件夹
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv') and filename not in file_have_read:
                print(f"读取文件{filename}")
                file_path = os.path.join(folder_path, filename)
                df = pd.read_csv(file_path, encoding="gbk")
                # cls.data_server.update_station_order(df=df, table_name=station_order_table_kuaiman)
                self.data_server.update_station_order(df=StationOrderHandler.kuaiman_old(df),
                                                     table_name=station_order_table_processed)
                file_new_read.add(filename)
        file_new_read = list(file_new_read)
        self.update_file_have_read(platform, file_new_read)

    def kuaiman_new(self):
        platform = station_order_table_kuaiman
        folder_path = entrance_kuaiman_new
        # file_have_read = self.get_file_have_read(platform)
        # file_new_read = set()
        # 遍历文件夹
        for filename in os.listdir(folder_path):
            if filename.endswith('.xlsx') and not (filename.startswith('~') or filename.startswith('$')):
                print(f"读取文件{filename}")
                file_path = os.path.join(folder_path, filename)
                df = pd.read_excel(file_path, skiprows=2, engine="openpyxl")
                # cls.data_server.update_station_order(df=df, table_name=station_order_table_kuaiman)
                self.data_server.update_station_order(df=StationOrderHandler.kuaiman_new(df),
                                                     table_name=station_order_table_processed)
        #         file_new_read.add(filename)
        # file_new_read = list(file_new_read)
        # self.update_file_have_read(platform, file_new_read)


