# from StationOrderServer import StationOrderServer,station_order_table_xiaoju,station_order_table_huitian
# server = StationOrderServer()
# df = server.get_station_order("中核汇天佛山禅城魁奇二路充电站",station_order_table_huitian,start_year=2024,start_month=1,start_day=28)
# print(1)

from FinancialReportGetter import FinancialReportGetter

df = FinancialReportGetter.get_full_order_by_date_station(2024,3,3, "中核汇天T3江北机场东路充电站")
print(11111111)