import os
import pandas as pd

_current_path = os.getcwd()


def _mk_dirs(path):
    path = _current_path+path
    if not os.path.exists(path):
        os.makedirs(path)  # 如果文件夹不存在，就创建它
        print(f"""文件夹{path}不存在，已为您创建。""")
    return path

database_station_order = _mk_dirs("\\Database")+"\\StationOrder.db"

entrance_huitian = _mk_dirs("\\Entrance\\汇天\\")
entrance_xiaoju = _mk_dirs("\\Entrance\\小桔\\")
entrance_xingxing_old = _mk_dirs("\\Entrance\\星星-旧\\")
entrance_xingxing_new = _mk_dirs("\\Entrance\\星星-新\\")
entrance_kuaiman_new = _mk_dirs("\\Entrance\\快满-新\\")
entrance_kuaiman_old = _mk_dirs("\\Entrance\\快满-旧\\")

station_order_table_huitian = "huitian"
station_order_table_xiaoju = "xiaoju"
station_order_table_kuaiman = "kuaiman"
station_order_table_xingxing = "xingxing"
station_order_table_processed = "processed"

_station_map_file = _current_path+"\\setting\\站点映射表.csv"
_df_station_map = pd.read_csv(_station_map_file, encoding="gbk")
_selected_columns = {'序号':'station_id', '平台名称': 'station_name_platform', "简称": "station_name", "平台": "platform",
                     "城市": "city", "总装机": "total_power", "上线时间": "start_time"}
_df_station_map = _df_station_map[list(_selected_columns.keys())]
_df_station_map = _df_station_map.rename(columns=_selected_columns)
_df_station_map["total_power"] = _df_station_map["total_power"].astype(int)
_df_station_map['station_id'] = _df_station_map['station_id'].astype(int)
_df_station_map['start_time'] = _df_station_map['start_time'].astype(str)


def station_map():
    return _df_station_map.copy(deep=True)

table_file_have_read = "file_have_read"
