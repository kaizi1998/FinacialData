from StationOrderUpdater import StationDataUpdater
from FinancialReportGenerator import FinancialReportGenerator

year = input("请输入年份（Year）: ")
month = input("请输入月份（Month）: ")
day = input("请输入日期（Day）: ")
# year, month, day = 2024, 2, 17

print("开始生成"+"{}年{}月{}日".format(year, month, day)+"报表")
stationDataUpdater = StationDataUpdater()
stationDataUpdater.huitian()
stationDataUpdater.kuaiman_old()
stationDataUpdater.kuaiman_new()
stationDataUpdater.xiaoju()
stationDataUpdater.xingxing()
FinancialReportGenerator.generate_financial_report(int(year), int(month), int(day))
FinancialReportGenerator.generate_txt(int(year), int(month), int(day))
print("生成完成")
input("按任意键退出...")
